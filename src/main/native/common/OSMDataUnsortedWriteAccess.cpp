#include "tpposmtilegen_common.h"
#include "UInt64SetMergeOperator.h"

#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>

#define TBB_USE_EXCEPTIONS (0)

#include <algorithm>
#include <atomic>
#include <execution>
#include <new>
#include <utility>
#include <vector>

#include <cassert>
#include <sys/mman.h>

#include <iostream>

class data_t {
public:
    uint32_t version;
    uint32_t size;
    char data[];
};

static_assert(sizeof(data_t) == sizeof(uint32_t) * 2);

class keyvalue_t {
public:
    uint64_t key;
    std::atomic<const data_t*> _value;

    const data_t* value_ptr() const noexcept { return _value.load(); }

    constexpr auto operator <(const keyvalue_t& other) const noexcept {
        if (key == 0 && other.key == 0) return key < other.key;
        else if (key == 0 && other.key != 0) return true;
        else if (key != 0 && other.key == 0) return false;
        else return false;
    }
    constexpr auto operator >(const keyvalue_t& other) const noexcept { return other < *this; }
};

static_assert(sizeof(keyvalue_t) == sizeof(uint64_t) * 2);
static_assert(sizeof(jlong) >= sizeof(ptrdiff_t));

static void throwOutOfMemory(JNIEnv* env, const std::bad_alloc& e) noexcept {
    if (!env->ExceptionCheck()) {
        env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), e.what());
    }
}

static void throwRocksdbException(JNIEnv* env, const rocksdb::Status& status) noexcept {
    if (!env->ExceptionCheck()) {
        std::string msg = status.ToString();
        env->ThrowNew(env->FindClass("org/rocksdb/RocksDBException"), msg.c_str());
    }
}

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_OSMDataUnsortedWriteAccess_init
        (JNIEnv *env, jclass cla) {
}

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_OSMDataUnsortedWriteAccess_trySwapIndexEntry
        (JNIEnv *env, jobject instance, keyvalue_t* indexBegin, uint64_t key, const data_t* value) {
    if (indexBegin[key].key == 0) {
        indexBegin[key].key = key;
    }

    const data_t* currentValue = indexBegin[key].value_ptr();
    do {
        if (currentValue != nullptr && currentValue->version >= value->version) {
            DEBUG_MSG("not downgrading key " << key << " from version " << currentValue->version << " to " << value->version);
            return -1;
        }
    } while (!indexBegin[key]._value.compare_exchange_strong(currentValue, value));

    if (currentValue != nullptr) {
        jint size = currentValue->size;
        //free(const_cast<data_t*>(currentValue));
        ::operator delete(static_cast<void*>(const_cast<data_t*>(currentValue)), size + sizeof(data_t));
        return size;
    } else {
        return 0;
    }
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_OSMDataUnsortedWriteAccess_appendKeys
        (JNIEnv *env, jobject instance, rocksdb::SstFileWriter* writer, const keyvalue_t* begin, const keyvalue_t* end) {
    try {
        assert(begin < end);

        jlong writtenKeys = 0;

        for (const keyvalue_t* it = begin; it != end; it++) {
            uint64_t key = it->key;
            if (key == 0) {
                continue;
            }

            uint64be key_be = key;

            const data_t* value = it->value_ptr();
            rocksdb::Status status = writer->Put(
                rocksdb::Slice(reinterpret_cast<const char*>(&key_be), sizeof(uint64be)),
                rocksdb::Slice(reinterpret_cast<const char*>(&value->data[0]), value->size));

            //free(const_cast<data_t*>(value));
            ::operator delete(static_cast<void*>(const_cast<data_t*>(value)), value->size + sizeof(data_t));

            if (!status.ok()) {
                throwRocksdbException(env, status);
                return false;
            }

            writtenKeys++;
        }

        return writtenKeys;
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
        return false;
    }
}

}
