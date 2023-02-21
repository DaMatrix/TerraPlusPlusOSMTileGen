#include <bit>

template<std::endian ORDER>
class bo_uint64 {
    static_assert(std::endian::big == std::endian::native || std::endian::little == std::endian::native);

private:
    constexpr static bool is_native = ORDER == std::endian::native;

    uint64_t _payload;

public:
    constexpr bo_uint64() noexcept : _payload() {}
    constexpr bo_uint64(uint64_t payload) noexcept : _payload(is_native ? payload : __builtin_bswap64(payload)) {}
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

using uint64be = bo_uint64<std::endian::big>;
static_assert(sizeof(uint64be) == sizeof(uint64_t));

using uint64le = bo_uint64<std::endian::little>;
static_assert(sizeof(uint64le) == sizeof(uint64_t));
