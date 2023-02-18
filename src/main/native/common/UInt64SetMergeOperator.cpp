#include "tpposmtilegen_common.h"
#include <lib-rocksdb/include/rocksdb/merge_operator.h>

#include <algorithm>
#include <bit>
#include <exception>
#include <ranges>
#include <set>
#include <vector>

#include <iostream>

#ifdef NATIVES_DEBUG
#define DEBUG_MSG(msg) std::cout << msg << std::endl;
#else
#define DEBUG_MSG(msg)
#endif

template<std::endian ORDER>
class bo_uint64 {
    static_assert(std::endian::big == std::endian::native || std::endian::little == std::endian::native);

private:
    constexpr static bool is_native = ORDER == std::endian::native;

    uint64_t _payload;

public:
    constexpr bo_uint64() noexcept : _payload() {}
    constexpr bo_uint64(uint64_t payload) noexcept : _payload(payload) {}
    constexpr bo_uint64(const bo_uint64& other) noexcept = default;
    constexpr bo_uint64(bo_uint64&& other) noexcept = default;

    constexpr ~bo_uint64() noexcept = default;

    constexpr bo_uint64& operator =(const bo_uint64& other) noexcept = default;
    constexpr bo_uint64& operator =(bo_uint64&& other) noexcept = default;

    constexpr operator uint64_t() const noexcept {
        if constexpr (is_native)
            return _payload;
        else
            return __builtin_bswap64(_payload);
    }

    constexpr bo_uint64& operator =(uint64_t value) noexcept {
        if constexpr (is_native) {
            _payload = value;
        } else {
            _payload = __builtin_bswap64(value);
        }
        return *this;
    }
};

using uint64le = bo_uint64<std::endian::little>;
static_assert(sizeof(uint64le) == sizeof(uint64_t));

class operand_t {
public:
    uint64le add_count;
    uint64le del_count;

    struct range_container {
        const uint64le* _begin;
        const uint64le* _end;

    public:
        constexpr range_container(const uint64le* begin, const uint64le* end) noexcept : _begin(begin), _end(end) {}

        auto begin() noexcept { return _begin; }
        constexpr auto begin() const noexcept { return _begin; }
        auto end() noexcept { return _end; }
        constexpr auto end() const noexcept { return _end; }

        constexpr bool empty() const noexcept { return _begin == _end; }
    };

    constexpr uint64_t total_count() const noexcept {
        return add_count + del_count;
    }

    constexpr size_t total_size_with_headers() const noexcept {
        return sizeof(uint64le) * (2 + total_count());
    }

    constexpr void validate(size_t total_size) const noexcept {
        assert(total_size == total_size_with_headers());
        assert(std::ranges::is_sorted(add()));
        assert(std::ranges::is_sorted(del()));
    }

    range_container add() const noexcept {
        return {
            reinterpret_cast<const uint64le*>(this) + 2,
            reinterpret_cast<const uint64le*>(this) + 2 + add_count
        };
    }

    range_container del() const noexcept {
        return {
            reinterpret_cast<const uint64le*>(this) + 2 + add_count,
            reinterpret_cast<const uint64le*>(this) + 2 + add_count + del_count
        };
    }
};

static_assert(sizeof(operand_t) == sizeof(uint64le) * 2);

class UInt64SetMergeOperator : public rocksdb::MergeOperator {
    auto collection_to_string(const auto& src) const {
        std::string buf = "[";
        for (const auto& val : src) {
            buf += std::to_string(val);
            buf += ",";
        }
        if (buf.size() > 1) {
            buf[buf.size() - 1] = ']';
        } else {
            buf += ']';
        }
        return buf;
    }

    template<typename T>
    constexpr static void set_union(auto&& a, auto&& b, std::vector<T>& dst) {
        dst.resize(std::ranges::size(a) + std::ranges::size(b));
        dst.resize(-(dst.begin() - std::ranges::set_union(a, b, dst.begin()).out));
    }

    template<typename T>
    constexpr static void set_difference(auto&& a, auto&& b, std::vector<T>& dst) {
        dst.resize(std::ranges::size(a) + std::ranges::size(b));
        dst.resize(-(dst.begin() - std::ranges::set_difference(a, b, dst.begin()).out));
    }

public:
    virtual ~UInt64SetMergeOperator() = default;

    bool FullMergeV2(const MergeOperationInput& merge_in, MergeOperationOutput* merge_out) const override {
        try {
#if true
            std::vector<uint64le> state;
            if (merge_in.existing_value) {
                const char* existing_data = merge_in.existing_value->data();
                size_t size = merge_in.existing_value->size();
                assert(size % sizeof(uint64le) == 0);

                state.insert(state.begin(), reinterpret_cast<const uint64le*>(existing_data), reinterpret_cast<const uint64le*>(existing_data + size));
                assert(std::ranges::is_sorted(state));
            }

            DEBUG_MSG("merge: existing value: " << collection_to_string(state));

            std::vector<uint64le> tmp(state.size());
            for (const auto& operand : merge_in.operand_list) {
                DEBUG_MSG("merge: operand size is " << operand.size());

                const operand_t& op = *reinterpret_cast<const operand_t*>(operand.data());
                op.validate(operand.size());

                DEBUG_MSG("merge: operand: add " << collection_to_string(op.add()) << ", del " << collection_to_string(op.del()));

                //delete
                tmp.clear();
                set_difference(state, op.del(), tmp);

                //add
                set_union(tmp, op.add(), state);
            }

            DEBUG_MSG("merge: resulting value: " << collection_to_string(state));

            merge_out->new_value.resize(state.size() * sizeof(uint64le));
            std::ranges::copy(state, reinterpret_cast<uint64le*>(&merge_out->new_value[0]));
#else
            std::set<uint64_t> state;
            if (merge_in.existing_value) {
                const char* existing_data = merge_in.existing_value->data();
                size_t size = merge_in.existing_value->size();
                assert(size % sizeof(uint64le) == 0);

                state = std::set<uint64_t>(reinterpret_cast<const uint64le*>(existing_data), reinterpret_cast<const uint64le*>(existing_data + size));
            }

            DEBUG_MSG("merge: existing value: " << collection_to_string(state));

            for (const auto& operand : merge_in.operand_list) {
                const operand_t& op = *reinterpret_cast<const operand_t*>(operand.data());
                op.validate(operand.size());

                DEBUG_MSG("merge: operand: add " << collection_to_string(op.add()) << ", del " << collection_to_string(op.del()));

                //delete
                for (auto val : op.del()) {
                    state.erase(val);
                }

                //add
                state.insert(op.add().begin(), op.add().end());
            }

            DEBUG_MSG("merge: resulting value: " << collection_to_string(state));

            merge_out->new_value.resize(state.size() * sizeof(uint64le));
            size_t i = 0;
            for (const auto& val : state) {
                *reinterpret_cast<uint64le*>(&merge_out->new_value[i * sizeof(uint64le)]) = val;
                i++;
            }
#endif
            return true;
        } catch (const std::exception& e) {
            std::cerr << e.what() << std::endl;
        }
        return false;
    }

    bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand, const rocksdb::Slice& right_operand, std::string* new_value, rocksdb::Logger* logger) const override {
        DEBUG_MSG("partialmerge: left_operand size is " << left_operand.size());
        const operand_t& lop = *reinterpret_cast<const operand_t*>(left_operand.data());
        lop.validate(left_operand.size());

        DEBUG_MSG("partialmerge: right_operand size is " << right_operand.size());
        const operand_t& rop = *reinterpret_cast<const operand_t*>(right_operand.data());
        rop.validate(right_operand.size());

        DEBUG_MSG("partialmerge: lop: add " << collection_to_string(lop.add()) << ", del " << collection_to_string(lop.del()));
        DEBUG_MSG("partialmerge: rop: add " << collection_to_string(rop.add()) << ", del " << collection_to_string(rop.del()));

        //this is big enough for any possible argument values
        size_t capacity = lop.total_count() + rop.total_count();

        std::vector<uint64le> merged_adds(capacity);
        std::vector<uint64le> merged_dels(capacity);
        std::vector<uint64le> tmp(capacity);

        //merged_adds = (lop.add() - rop.del()) + rop.add()
        set_difference(lop.add(), rop.del(), tmp);
        set_union(tmp, rop.add(), merged_adds);

        //merged_dels = (lop.del() - rop.add()) + rop.del()
        set_difference(lop.del(), rop.add(), tmp);
        set_union(tmp, rop.del(), merged_dels);

        operand_t tmp_op = { .add_count = merged_adds.size(), .del_count = merged_dels.size() };
        new_value->resize(tmp_op.total_size_with_headers());
        *reinterpret_cast<operand_t*>(&(*new_value)[0]) = tmp_op;

        auto pos = std::ranges::copy(merged_adds, reinterpret_cast<uint64le*>(&(*new_value)[sizeof(operand_t)])).out;
        std::ranges::copy(merged_dels, pos);

        const operand_t& op = *reinterpret_cast<const operand_t*>(&(*new_value)[0]);
        op.validate(new_value->size());

        DEBUG_MSG("partialmerge: result: add " << collection_to_string(op.add()) << ", del " << collection_to_string(op.del()) << " new_value->size()=" << new_value->size());
        return true;
    }

    /*bool PartialMergeMulti(const rocksdb::Slice& key, const std::deque<rocksdb::Slice>& operand_list, std::string* new_value, rocksdb::Logger* logger) const override {
        DEBUG_MSG("partialmergemulti: " << operand_list.size() << " operands");

        if (operand_list.size() == 1) {
            return false;
        }

        rocksdb::Slice temp_slice(operand_list[0]);

        for (size_t i = 1; i < operand_list.size(); ++i) {
            auto& operand = operand_list[i];
            std::string temp_value;
            if (!PartialMerge(key, temp_slice, operand, &temp_value, logger)) {
                return false;
            }
            std::swap(temp_value, *new_value);
            temp_slice = rocksdb::Slice(*new_value);
        }

        return true;
    }

    bool AllowSingleOperand() const override {
        return true;
    }*/

    const char* Name() const override {
        return "UInt64SetMergeOperator";
    }
};

static_assert(sizeof(jlong) == sizeof(UInt64SetMergeOperator*));

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetMergeOperator_init
        (JNIEnv *env, jclass cla) {
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetMergeOperator_create
        (JNIEnv *env, jclass cla) {
    return static_cast<jlong>(reinterpret_cast<size_t>(new std::shared_ptr<UInt64SetMergeOperator>(std::make_shared<UInt64SetMergeOperator>())));
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetMergeOperator_disposeInternal
        (JNIEnv *env, jobject instance, jlong ptr) {
    delete reinterpret_cast<std::shared_ptr<UInt64SetMergeOperator>*>(ptr);
}

}
