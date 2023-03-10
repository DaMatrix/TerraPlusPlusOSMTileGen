namespace {
    struct __attribute__((packed)) element_t {
    public:
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
}

static_assert(sizeof(element_t) == sizeof(uint64_t) + sizeof(int32_t));
