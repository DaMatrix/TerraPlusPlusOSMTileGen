#include "tpposmtilegen_common.h"
#include "UInt64SetMergeOperator.h"

#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>

#include <algorithm>
#include <new>
#include <utility>

#include <cassert>
#include <sys/mman.h>

#include <parallel/algorithm>

#include <iostream>

class data_t {
public:
    int32_t size;
    char data[];
};

class keyvalue_t {
public:
    uint64le key;
    uint64le _value;

    data_t* value_ptr() const noexcept { return reinterpret_cast<data_t*>(static_cast<uint64_t>(_value)); }

    constexpr auto operator <(const keyvalue_t& other) const noexcept { return key < other.key; }
    constexpr auto operator >(const keyvalue_t& other) const noexcept { return other < *this; }
};

static_assert(sizeof(data_t) == sizeof(uint32_t));

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

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64BlobUnsortedWriteAccess_sortBuffer
        (JNIEnv *env, jobject instance, jlong _addr, jlong _size, jboolean parallel) {
    try {
        assert(_size % sizeof(keyvalue_t) == 0);

        keyvalue_t* addr = reinterpret_cast<keyvalue_t*>(_addr);
        ptrdiff_t size = static_cast<ptrdiff_t>(_size / sizeof(keyvalue_t));

        madvise(addr, size * sizeof(keyvalue_t), MADV_WILLNEED);

        if (parallel) {
            __gnu_parallel::sort(&addr[0], &addr[size]);
        } else {
            std::sort(&addr[0], &addr[size]);
        }

        madvise(addr, size * sizeof(keyvalue_t), MADV_NORMAL);
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
    }
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64BlobUnsortedWriteAccess_appendKeys
        (JNIEnv *env, jobject instance, rocksdb::SstFileWriter* writer, const keyvalue_t* begin, const keyvalue_t* end) {
    try {
        assert(begin < end);

        jlong writtenKeys = 0;

        for (const keyvalue_t* it = begin; it != end; it++) {
            uint64_t key = it->key;
            const data_t* value = it->value_ptr();

            uint64be key_be = key;

            rocksdb::Status status = rocksdb::Status::OK();
            status = writer->Put(
                    rocksdb::Slice(reinterpret_cast<const char*>(&key_be), sizeof(uint64be)),
                    rocksdb::Slice(reinterpret_cast<const char*>(&value->data[0]), value->size));

            //free(const_cast<data_t*>(value));
            ::operator delete(static_cast<void*>(const_cast<data_t*>(value)), value->size + sizeof(data_t));

            if (!status.ok()) {
                throwRocksdbException(env, status);
                return 0;
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
