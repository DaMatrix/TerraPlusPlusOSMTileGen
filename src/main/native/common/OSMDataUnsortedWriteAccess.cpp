#include "tpposmtilegen_common.h"
#include "UInt64SetMergeOperator.h"

#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>

#include <atomic>
#include <new>
#include <utility>

#include <cassert>
#include <sys/mman.h>

#include <iostream>

namespace {
    class data_t {
    public:
        uint32_t version;
        int32_t size; //negative if removed
        char data[];
    };

    static_assert(sizeof(data_t) == sizeof(uint32_t) + sizeof(int32_t));

    class keyvalue_t {
    public:
        uint64_t key;
        std::atomic<const data_t*> _value;

        const data_t* value_ptr() const noexcept { return _value.load(); }
    };

    static_assert(sizeof(keyvalue_t) == sizeof(uint64_t) * 2);
    static_assert(sizeof(jlong) >= sizeof(ptrdiff_t));
}

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
        jint size = std::max(currentValue->size, 0);
        //free(const_cast<data_t*>(currentValue));
        ::operator delete(static_cast<void*>(const_cast<data_t*>(currentValue)), size + sizeof(data_t));
        return size;
    } else {
        return 0;
    }
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_OSMDataUnsortedWriteAccess_appendKeys
        (JNIEnv *env, jobject instance, rocksdb::SstFileWriter* writer, const keyvalue_t* begin, const keyvalue_t* end, jboolean assumeEmpty) {
    try {
        assert(begin < end);

        jlong writtenKeys = 0;

        for (const keyvalue_t* it = begin; it != end; it++) {
            size_t free_interval = 16 << 20;
            if (((it - begin) & (free_interval - 1)) == 0) { // 4 MiKeys since last unmap, free up some pages
                //std::cout << "freeing " << free_interval << " index entries starting at " << begin << std::endl;
                auto res = madvise(const_cast<keyvalue_t*>(begin), (it - begin) * sizeof(keyvalue_t), MADV_DONTNEED);
                if (UNLIKELY(res < 0)) {
                    env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), "failed to unmap memory");
                    return 0;
                }
            }

            uint64_t key = it->key;
            const data_t* value = it->value_ptr();
            if (key == 0 && value == nullptr) { //also check if value is nullptr, because key 0 has a key of 0
                continue;
            }

            uint64be key_be = key;

            rocksdb::Status status = rocksdb::Status::OK();
            if (value->size >= 0) { //put
                status = writer->Put(
                        rocksdb::Slice(reinterpret_cast<const char*>(&key_be), sizeof(uint64be)),
                        rocksdb::Slice(reinterpret_cast<const char*>(&value->data[0]), value->size));
            } else { //delete
                if (assumeEmpty) { //we assume the db is empty, therefore we can safely omit any deletes
                    status = rocksdb::Status::OK();
                } else { //the db isn't empty, thus we need to include a deletion entry in case the key is already present
                    status = writer->Delete(
                            rocksdb::Slice(reinterpret_cast<const char*>(&key_be), sizeof(uint64be)));
                }
            }

            //free(const_cast<data_t*>(value));
            ::operator delete(static_cast<void*>(const_cast<data_t*>(value)), std::max(value->size, 0) + sizeof(data_t));

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
