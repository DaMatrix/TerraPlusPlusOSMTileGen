#include "tpposmtilegen_common.h"
#include "byte_order.h"
#include <lib-rocksdb/include/rocksdb/merge_operator.h>

#include <algorithm>
#include <exception>
#include <map>
#include <ranges>
#include <vector>
#include <utility>

#include <iostream>

struct __attribute__((packed)) element_t {
    uint64le key;
    int32le value_size; //negative if being removed
    char value_data[];

    const element_t* next() const noexcept {
        return reinterpret_cast<const element_t*>(&value_data[std::max({ static_cast<int32_t>(value_size), 0})]);
    }

    std::string_view element_as_view() const noexcept {
        return { reinterpret_cast<const char*>(this), sizeof(element_t) + std::max({ static_cast<int32_t>(value_size), 0}) };
    }

    std::string_view value_as_view() const noexcept {
        assert(value_size >= 0);
        return { value_data, static_cast<size_t>(value_size) };
    }
};

static_assert(sizeof(element_t) == sizeof(uint64_t) + sizeof(int32_t));

class UInt64ToBlobMapMergeOperator : public rocksdb::MergeOperator {
    static auto collection_to_string(const auto& src) {
        std::string buf = "[";
        for (const auto& entry : src) {
            auto* element = reinterpret_cast<const element_t*>(entry.second.data());
            buf += '[';
            buf += std::to_string(element->key);
            buf += '=';
            if (element->value_size >= 0) {
                buf += '\"';
                buf += element->value_as_view();
                buf += '\"';
            } else {
                buf += "(deleted)";
            }
            buf += "],";
        }
        if (buf.size() > 1) {
            buf[buf.size() - 1] = ']';
        } else {
            buf += ']';
        }
        return buf;
    }

    static void for_each_element(const rocksdb::Slice& slice, auto action) {
        const char* data = slice.data();
        size_t size = slice.size();

        auto* begin = reinterpret_cast<const element_t*>(data);
        auto* end = reinterpret_cast<const element_t*>(data + size);

        uint64_t prev_key = 0;
        for (const auto* it = begin; it != end; it = it->next()) {
            assert(it < end);

            /*DEBUG_MSG("processing element at " << it << " (" << (reinterpret_cast<const char*>(it) - reinterpret_cast<const char*>(begin)) << " bytes in)\n"
                      "    key=" << it->key << " value_size=" << it->value_size);*/

            //ensure keys are in order
            assert(it == begin || prev_key < it->key);
            prev_key = it->key;

            action(it);
        }
    }

    static std::map<uint64_t, std::string_view> decode_state(const rocksdb::Slice& slice) {
        std::map<uint64_t, std::string_view> state;
        for_each_element(slice, [&](const element_t* it) {
            state.insert_or_assign(it->key, it->element_as_view());
        });
        return state;
    }

    static void write_to_string(const std::map<uint64_t, std::string_view>& state, std::string* dst) {
        assert(dst->empty());

        //compute total size of all elements
        size_t total_size = 0;
        for (const auto& entry : state) {
            total_size += entry.second.size();
        }

        //reserve capacity for everything, then append everything to the output value
        dst->reserve(total_size);
        for (const auto& entry : state) {
            dst->append(entry.second);
        }
    }

public:
    virtual ~UInt64ToBlobMapMergeOperator() = default;

    bool FullMergeV2(const MergeOperationInput& merge_in, MergeOperationOutput* merge_out) const override {
        try {
            std::map<uint64_t, std::string_view> state;

            if (merge_in.existing_value) {
                for_each_element(*merge_in.existing_value, [&](const element_t* it) {
                    assert(it->value_size >= 0); //existing value shouldn't contain any deletes
                    state.insert_or_assign(it->key, it->element_as_view());
                });
            }

            DEBUG_MSG("merge: existing value: " << (merge_in.existing_value ? collection_to_string(state) : "(nullptr)"));

            for (const auto& operand : merge_in.operand_list) {
                DEBUG_MSG("merge: operand: " << collection_to_string(decode_state(operand)));

                for_each_element(operand, [&](const element_t* it) {
                    if (it->value_size >= 0) { //add
                        state.insert_or_assign(it->key, it->element_as_view());
                    } else { //delete
                        state.erase(it->key);
                    }
                });
            }

            DEBUG_MSG("merge: resulting value: " << collection_to_string(state));

            write_to_string(state, &merge_out->new_value);
            return true;
        } catch (const std::exception& e) {
            std::cerr << e.what() << std::endl;
        }
        return false;
    }

    bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand, const rocksdb::Slice& right_operand, std::string* new_value, rocksdb::Logger* logger) const override {
        DEBUG_MSG("partialmerge: left_operand size is " << left_operand.size());
        DEBUG_MSG("partialmerge: lop: " << collection_to_string(decode_state(left_operand)));

        DEBUG_MSG("partialmerge: right_operand size is " << right_operand.size());
        DEBUG_MSG("partialmerge: rop: " << collection_to_string(decode_state(right_operand)));

        std::map<uint64_t, std::string_view> state;
        for_each_element(left_operand, [&](const element_t* it) {
            state.insert_or_assign(it->key, it->element_as_view());
        });
        for_each_element(right_operand, [&](const element_t* it) {
            state.insert_or_assign(it->key, it->element_as_view());
        });

        write_to_string(state, new_value);

        DEBUG_MSG("partialmerge: result: " << collection_to_string(state) << " new_value->size()=" << new_value->size());
        return true;
    }

    bool PartialMergeMulti(const rocksdb::Slice& key, const std::deque<rocksdb::Slice>& operand_list, std::string* new_value, rocksdb::Logger* logger) const override {
        DEBUG_MSG("partialmergemulti: " << operand_list.size() << " operands");

        std::map<uint64_t, std::string_view> state;
        for (const auto& operand : operand_list) {
            DEBUG_MSG("partialmergemulti: operand: " << collection_to_string(decode_state(operand)));

            for_each_element(operand, [&](const element_t* it) {
                state.insert_or_assign(it->key, it->element_as_view());
            });
        }

        DEBUG_MSG("partialmergemulti: resulting value: " << collection_to_string(state));
        write_to_string(state, new_value);
        return true;
    }

    bool AllowSingleOperand() const override {
        return true;
    }

    const char* Name() const override {
        return "UInt64ToBlobMapMergeOperator";
    }
};

static_assert(sizeof(jlong) == sizeof(UInt64ToBlobMapMergeOperator*));

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64ToBlobMapMergeOperator_init
        (JNIEnv *env, jclass cla) {
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64ToBlobMapMergeOperator_create
        (JNIEnv *env, jclass cla) {
    return static_cast<jlong>(reinterpret_cast<size_t>(new std::shared_ptr<UInt64ToBlobMapMergeOperator>(std::make_shared<UInt64ToBlobMapMergeOperator>())));
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64ToBlobMapMergeOperator_disposeInternal
        (JNIEnv *env, jobject instance, jlong ptr) {
    delete reinterpret_cast<std::shared_ptr<UInt64ToBlobMapMergeOperator>*>(ptr);
}

}
