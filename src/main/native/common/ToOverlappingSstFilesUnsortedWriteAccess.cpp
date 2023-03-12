#include "tpposmtilegen_common.h"
#include "byte_order.h"

#include <lib-rocksdb/include/rocksdb/merge_operator.h>
#include <lib-rocksdb/include/rocksdb/options.h>
#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>

#include <algorithm>
#include <deque>
#include <map>
#include <new>
#include <utility>
#include <vector>

#include <cassert>

static void checkRocksdbStatus(JNIEnv* env, const rocksdb::Status& status, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE()) {
    if (UNLIKELY(!status.ok())) {
        std::string msg = status.ToString();
        throwException(env, "org/rocksdb/RocksDBException", msg.c_str(), function, file, line);
    }
}

namespace {
    enum operation_t {
        PUT,
        MERGE,
        DELETE,
    };

    class update_t {
    public:
        std::string_view key;
        std::string_view value;
        operation_t operation;
    };

    class state_t {
    public:
        std::string data_buffer;
        std::vector<update_t> updates;

        std::shared_ptr<rocksdb::MergeOperator> merge_operator;

        state_t(const rocksdb::Options* options) : merge_operator(options->merge_operator) {
            if (UNLIKELY(options->comparator != rocksdb::BytewiseComparator())) {
                throw std::runtime_error("only BytewiseComparator is supported!");
            }
        }

        void on_data_buffer_resized(const char* old_data_base, const char* new_data_base) noexcept {
            assert(old_data_base != new_data_base);

            for (update_t& update : this->updates) {
                update.key = { update.key.data() - old_data_base + new_data_base, update.key.size() };
                update.value = { update.value.data() - old_data_base + new_data_base, update.value.size() };
            }
        }

        void reserve_capacity(size_t capacity) {
            const char* old_data_base = this->data_buffer.data();
            this->data_buffer.reserve(capacity);
            const char* new_data_base = this->data_buffer.data();

            if (UNLIKELY(old_data_base != new_data_base)) { //the string's internal buffer has been resized, update the base pointers
                on_data_buffer_resized(old_data_base, new_data_base);
            }
        }

        std::string_view append_data_assuming_sufficient_reserved_space(const std::string_view& new_data) {
            assert(this->data_buffer.capacity() - this->data_buffer.size() >= new_data.size() && "insufficient free capacity!");

            size_t old_size = this->data_buffer.size();

            const char* old_data_base = this->data_buffer.data();
            this->data_buffer.append(new_data);
            const char* new_data_base = this->data_buffer.data();
            assert(old_data_base == new_data_base && "data buffer was resized even though sufficient capacity was already reserved!");

            return { this->data_buffer.data() + old_size, new_data.size() };
        }

        void append_update(const std::string_view& key, const std::string_view& value, operation_t operation) {
            this->reserve_capacity(key.size() + value.size());

            this->updates.push_back({
                this->append_data_assuming_sufficient_reserved_space(key),
                this->append_data_assuming_sufficient_reserved_space(value),
                operation
            });
        }

        void clear() {
            this->data_buffer.clear();
            this->updates.clear();
        }

        void flush(JNIEnv* env, rocksdb::SstFileWriter* writer) {
            assert(!this->updates.empty());

            //sort the pending updates, using a stable sort to preserve order when applying merge operators and such
            std::stable_sort(this->updates.begin(), this->updates.begin(), [](const update_t& a, const update_t& b) { return a.key < b.key; });

            rocksdb::Logger* logger = nullptr;

            //these are placed outside the loop in order to avoid reallocating their internal buffers a bazillion times
            std::vector<rocksdb::Slice> queued_merge_operands;
            std::deque<rocksdb::Slice> queued_merge_operands_deque;
            std::string new_value;

            for (auto it = this->updates.begin(); it != this->updates.end(); ) {
                std::string_view current_key = it->key;

                std::string_view* current_put_value = nullptr;
                std::string_view* current_delete_value = nullptr;
                queued_merge_operands.clear();

                //process every pending update with the same key
                do {
                    switch (it->operation) {
                        case operation_t::PUT: //overwrite any previously buffered values
                            current_put_value = &it->value;
                            current_delete_value = nullptr;
                            queued_merge_operands.clear();
                            break;
                        case operation_t::DELETE: //overwrite any previously buffered values
                            current_put_value = nullptr;
                            current_delete_value = &it->value;
                            queued_merge_operands.clear();
                            break;
                        case operation_t::MERGE:
                            queued_merge_operands.emplace_back(it->value.data(), it->value.size());
                            break;
                        default:
                            __builtin_unreachable();
                    }

                    ++it;
                } while (it != this->updates.end() && it->key == current_key);

                if (current_put_value != nullptr) { //this is a put operation
                    if (queued_merge_operands.empty()) { //there are no queued merge operands which come afterwards, so we can do a simple put
                        checkRocksdbStatus(env, writer->Put(current_key, *current_put_value));
                    } else { //there are some merge operands which need to be applied on top of the put
                        assert(false && "don't know how to merge with an initial put"); //TODO

                        rocksdb::Slice current_put_value_slice = *current_put_value;
                        rocksdb::Slice tmp_result_operand(nullptr, 0);
                        const rocksdb::MergeOperator::MergeOperationInput merge_in(current_key, &current_put_value_slice, queued_merge_operands, logger);
                        rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, tmp_result_operand);

                        bool fullMergeResult = merge_operator->FullMergeV2(merge_in, &merge_out);
                        assert(fullMergeResult && "merge_operator->FullMergeV2() returned false?!?");

                        rocksdb::Slice result;
                        if (tmp_result_operand.data() != nullptr) { //result is an existing operand
                            result = tmp_result_operand;
                        } else {
                            result = new_value;
                        }
                        checkRocksdbStatus(env, writer->Put(current_key, result));
                    }
                } else if (current_delete_value != nullptr) { //this is a delete operation
                    if (queued_merge_operands.empty()) { //there are no queued merge operands which come afterwards, so we can do a simple delete
                        checkRocksdbStatus(env, writer->Delete(current_key));
                    } else { //there are some merge operands which need to be applied on top of the delete
                        assert(false && "don't know how to merge with an initial delete"); //TODO

                        rocksdb::Slice tmp_result_operand(nullptr, 0);
                        const rocksdb::MergeOperator::MergeOperationInput merge_in(current_key, /*old value is nullptr because it was deleted*/ nullptr, queued_merge_operands, logger);
                        rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, tmp_result_operand);

                        bool fullMergeResult = merge_operator->FullMergeV2(merge_in, &merge_out);
                        assert(fullMergeResult && "merge_operator->FullMergeV2() returned false?!?");

                        rocksdb::Slice result;
                        if (tmp_result_operand.data() != nullptr) { //result is an existing operand
                            result = tmp_result_operand;
                        } else {
                            result = new_value;
                        }
                        checkRocksdbStatus(env, writer->Put(current_key, result));
                    }
                } else {
                    assert(!queued_merge_operands.empty() && "somehow we processed a key with no operations?!?");

                    //there are either only merge operands, or a delete followed only by merge operands. in either case, we only
                    // have to do a partial merge of the operands, then write the result out to the SST file as a single operand

                    if (queued_merge_operands.size() == 1) { //single operand, no partial merge is necessary
                        checkRocksdbStatus(env, writer->Merge(current_key, *queued_merge_operands.begin()));
                    } else { //multiple merge operands, do a partial merge
                        new_value.clear();
                        queued_merge_operands_deque.assign(queued_merge_operands.begin(), queued_merge_operands.end());

                        bool partialMergeResult = merge_operator->PartialMergeMulti(current_key, queued_merge_operands_deque, &new_value, logger);
                        assert(partialMergeResult && "merge_operator->PartialMergeMulti() returned false?!?");

                        checkRocksdbStatus(env, writer->Merge(current_key, new_value));
                    }
                }
            }

            this->clear();
        }
    };
}

JNI_FUNCTION_HEAD(jlong, net_daporkchop_tpposmtilegen_natives_ToOverlappingSstFilesUnsortedWriteAccess_createState0, jclass, jlong _optionsHandle)
    const rocksdb::Options* options = tpp::jlong_to_ptr<const rocksdb::Options*>(_optionsHandle);
    return tpp::ptr_to_jlong(new state_t(options));
JNI_FUNCTION_TAIL(0)

JNI_FUNCTION_HEAD(void, net_daporkchop_tpposmtilegen_natives_ToOverlappingSstFilesUnsortedWriteAccess_deleteState0, jclass, jlong _state)
    delete tpp::jlong_to_ptr<state_t*>(_state);
JNI_FUNCTION_TAIL()

JNI_FUNCTION_HEAD(void, net_daporkchop_tpposmtilegen_natives_ToOverlappingSstFilesUnsortedWriteAccess_put0, jclass, jlong _state, jlong _key, jint keySize, jlong _value, jint valueSize)
    tpp::jlong_to_ptr<state_t*>(_state)->append_update(
        std::string_view(tpp::jlong_to_ptr<const char*>(_key), keySize),
        std::string_view(tpp::jlong_to_ptr<const char*>(_value), valueSize),
        operation_t::PUT);
JNI_FUNCTION_TAIL()

JNI_FUNCTION_HEAD(void, net_daporkchop_tpposmtilegen_natives_ToOverlappingSstFilesUnsortedWriteAccess_merge0, jclass, jlong _state, jlong _key, jint keySize, jlong _value, jint valueSize)
    tpp::jlong_to_ptr<state_t*>(_state)->append_update(
        std::string_view(tpp::jlong_to_ptr<const char*>(_key), keySize),
        std::string_view(tpp::jlong_to_ptr<const char*>(_value), valueSize),
        operation_t::MERGE);
JNI_FUNCTION_TAIL()

JNI_FUNCTION_HEAD(void, net_daporkchop_tpposmtilegen_natives_ToOverlappingSstFilesUnsortedWriteAccess_delete0, jclass, jlong _state, jlong _key, jint keySize)
    tpp::jlong_to_ptr<state_t*>(_state)->append_update(
        std::string_view(tpp::jlong_to_ptr<const char*>(_key), keySize),
        std::string_view(),
        operation_t::DELETE);
JNI_FUNCTION_TAIL()

JNI_FUNCTION_HEAD(void, net_daporkchop_tpposmtilegen_natives_ToOverlappingSstFilesUnsortedWriteAccess_clear0, jclass, jlong _state)
    tpp::jlong_to_ptr<state_t*>(_state)->clear();
JNI_FUNCTION_TAIL()

JNI_FUNCTION_HEAD(void, net_daporkchop_tpposmtilegen_natives_ToOverlappingSstFilesUnsortedWriteAccess_flush0, jclass, jlong _state, jlong _writer)
    tpp::jlong_to_ptr<state_t*>(_state)->flush(env, tpp::jlong_to_ptr<rocksdb::SstFileWriter*>(_writer));
JNI_FUNCTION_TAIL()
