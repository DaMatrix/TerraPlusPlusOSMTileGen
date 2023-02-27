#include "tpposmtilegen_common.h"
#include "UInt64SetMergeOperator.h"

#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>

#define TBB_USE_EXCEPTIONS (0)

#include <algorithm>
#include <atomic>
#include <execution>
#include <utility>
#include <vector>

#include <cassert>
#include <sys/mman.h>

#include <iostream>

class data_t {
public:
    uint32le version;
    uint32le size;
    char data[];
};

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

JNIEXPORT jlongArray JNICALL Java_net_daporkchop_tpposmtilegen_natives_OSMDataUnsortedWriteAccess_partitionSortedRange
        (JNIEnv *env, jobject instance, jlong _addr, jlong _size, jlong _targetBlockSize) {
    try {
        assert(_size % sizeof(keyvalue_t) == 0);
        assert(_targetBlockSize % sizeof(keyvalue_t) == 0);

        keyvalue_t* addr = reinterpret_cast<keyvalue_t*>(_addr);
        ptrdiff_t size = static_cast<ptrdiff_t>(_size / sizeof(keyvalue_t));
        ptrdiff_t targetBlockSize = static_cast<ptrdiff_t>(_targetBlockSize / sizeof(keyvalue_t));

        madvise(addr, size * sizeof(keyvalue_t), MADV_RANDOM);

        keyvalue_t* blockStart = addr;
        keyvalue_t* totalEnd = addr + size;

        using element_t = std::pair<keyvalue_t*, ptrdiff_t>;
        static_assert(sizeof(element_t) == 2 * sizeof(jlong));
        std::vector<element_t> blocks;
#if true
        while (blockStart < totalEnd) {
            keyvalue_t* blockEnd;
            if (totalEnd - blockStart <= targetBlockSize) { //tail
                blockEnd = totalEnd;
            } else { //skip ahead until we run out of entries with the same key
                blockEnd = blockStart + targetBlockSize;
            }

            DEBUG_MSG("added block from " << ((void*) ((blockStart - addr) * sizeof(keyvalue_t))) << " to " << ((void*) ((blockEnd - addr) * sizeof(keyvalue_t)))
                      << " (keys " << blockStart[0].key << " to " << blockEnd[-1].key << ")");
            blocks.push_back({ blockStart, (blockEnd - blockStart) * sizeof(keyvalue_t) });
            blockStart = blockEnd;
        }
#else
        blocks.push_back({ addr, size * sizeof(keyvalue_t) });
#endif

        madvise(addr, size * sizeof(keyvalue_t), MADV_NORMAL);

        jlongArray array = env->NewLongArray(blocks.size() * 2);
        env->SetLongArrayRegion(array, 0, blocks.size() * 2, reinterpret_cast<jlong*>(&blocks[0]));
        return array;
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
        return nullptr;
    }
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
        return currentValue->size;
    } else {
        return 0;
    }
}

JNIEXPORT jboolean JNICALL Java_net_daporkchop_tpposmtilegen_natives_OSMDataUnsortedWriteAccess_appendKeys
        (JNIEnv *env, jobject instance, rocksdb::SstFileWriter* writer, const keyvalue_t* begin, const keyvalue_t* end) {
    try {
        assert(begin < end);

        bool any = false;

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

            if (!status.ok()) {
                throwRocksdbException(env, status);
                return false;
            }

            any = true;
        }

        return any;
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
        return false;
    }
}

}
