#include "tpposmtilegen_common.h"
#include "byte_order.h"

#include <algorithm>
#include <new>
#include <vector>
#include <cassert>

namespace {

class operand_t {
public:
    uint64le add_count;
    uint64le del_count;
    uint64le _data[];

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

    void validate(size_t total_size) const noexcept {
        assert(total_size == total_size_with_headers());
        assert(std::is_sorted(add().begin(), add().end()));
        assert(std::is_sorted(del().begin(), del().end()));
    }

    constexpr range_container add() const noexcept {
        return {
            //reinterpret_cast<const uint64le*>(this) + 2,
            //reinterpret_cast<const uint64le*>(this) + 2 + add_count
            &_data[0], &_data[add_count]
        };
    }

    constexpr range_container del() const noexcept {
        return {
            //reinterpret_cast<const uint64le*>(this) + 2 + add_count,
            //reinterpret_cast<const uint64le*>(this) + 2 + add_count + del_count
            &_data[add_count], &_data[add_count + del_count]
        };
    }

    static operand_t* add(uint64le value) {
        operand_t* result = (operand_t*) ::operator new(sizeof(operand_t) + sizeof(uint64le));
        result->add_count = 1;
        result->del_count = 0;
        result->_data[0] = value;
        return result;
    }

    static operand_t* add(const std::vector<uint64le>& adds) {
        assert(std::is_sorted(adds.begin(), adds.end()));

        operand_t* result = (operand_t*) ::operator new(sizeof(operand_t) + adds.size() * sizeof(uint64le));
        result->add_count = adds.size();
        result->del_count = 0;
        std::copy(adds.begin(), adds.end(), &result->_data[0]);
        return result;
    }

    static operand_t* del(uint64le value) {
        operand_t* result = (operand_t*) ::operator new(sizeof(operand_t) + sizeof(uint64le));
        result->add_count = 0;
        result->del_count = 1;
        result->_data[0] = value;
        return result;
    }

    static operand_t* del(const std::vector<uint64le>& dels) {
        assert(std::is_sorted(dels.begin(), dels.end()));

        operand_t* result = (operand_t*) ::operator new(sizeof(operand_t) + dels.size() * sizeof(uint64le));
        result->add_count = 0;
        result->del_count = dels.size();
        std::copy(dels.begin(), dels.end(), &result->_data[0]);
        return result;
    }
};

}

static_assert(sizeof(operand_t) == sizeof(uint64le) * 2);
