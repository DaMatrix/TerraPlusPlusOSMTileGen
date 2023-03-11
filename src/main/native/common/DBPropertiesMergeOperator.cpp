#include "tpposmtilegen_common.h"
#include "byte_order.h"
#include <lib-rocksdb/include/rocksdb/merge_operator.h>

#include <algorithm>
#include <exception>
#include <ranges>
#include <vector>
#include <utility>

#include <iostream>

static const rocksdb::Slice UINT64_KEY_PREFIX = "uint64__";
static const rocksdb::Slice UINT64_ADD_OP_PREFIX = "add_";

class DBPropertiesMergeOperator : public rocksdb::MergeOperator {
    static uint64_t decode_uint64(const char* data) noexcept {
        return static_cast<uint64_t>(*reinterpret_cast<const uint64le*>(data));
    }

    static void encode_uint64(std::string& data, uint64_t value) {
        uint64le value_le = value;
        data.append(std::string_view(reinterpret_cast<char*>(&value_le), sizeof(uint64_t)));
    }

public:
    virtual ~DBPropertiesMergeOperator() = default;

    bool FullMergeV2(const MergeOperationInput& merge_in, MergeOperationOutput* merge_out) const override {
        try {
            if (merge_in.key.starts_with(UINT64_KEY_PREFIX)) {
                uint64_t value = 0;

                if (merge_in.existing_value) {
                    assert(merge_in.existing_value->size() == sizeof(uint64_t));
                    value = decode_uint64(merge_in.existing_value->data());
                }

                for (const auto& operand : merge_in.operand_list) {
                    assert(operand.starts_with(UINT64_ADD_OP_PREFIX));
                    value += decode_uint64(operand.data() + UINT64_ADD_OP_PREFIX.size());
                }

                encode_uint64(merge_out->new_value, value);
                return true;
            }

            assert(false);
        } catch (const std::exception& e) {
            std::cerr << e.what() << std::endl;
        }
        return false;
    }

    /*bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand, const rocksdb::Slice& right_operand, std::string* new_value, rocksdb::Logger* logger) const override {
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
    }*/

    const char* Name() const override {
        return "DBPropertiesMergeOperator";
    }
};

static_assert(sizeof(jlong) == sizeof(DBPropertiesMergeOperator*));

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_DBPropertiesMergeOperator_init
        (JNIEnv *env, jclass cla) {
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_DBPropertiesMergeOperator_create
        (JNIEnv *env, jclass cla) {
    return static_cast<jlong>(reinterpret_cast<size_t>(new std::shared_ptr<DBPropertiesMergeOperator>(std::make_shared<DBPropertiesMergeOperator>())));
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_DBPropertiesMergeOperator_disposeInternal
        (JNIEnv *env, jobject instance, jlong ptr) {
    delete reinterpret_cast<std::shared_ptr<DBPropertiesMergeOperator>*>(ptr);
}

}
