#include "tpposmtilegen_common.h"
#include "UInt64SetMergeOperator.h"

#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>

#define TBB_USE_EXCEPTIONS (0)

#include <algorithm>
#include <execution>
#include <utility>
#include <vector>
#include <queue>

#include <cassert>
#include <sys/mman.h>

class keyvalue_t {
public:
    uint64le key;
    uint64le value;

    constexpr auto operator <(const keyvalue_t& other) const noexcept { return key < other.key || (key == other.key && value < other.value); }
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

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetUnsortedWriteAccess_init
        (JNIEnv *env, jclass cla) {
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetUnsortedWriteAccess_sortBuffer
        (JNIEnv *env, jobject instance, jlong _addr, jlong _size, jboolean parallel) {
    try {
        assert(_size % sizeof(keyvalue_t) == 0);

        keyvalue_t* addr = reinterpret_cast<keyvalue_t*>(_addr);
        ptrdiff_t size = static_cast<ptrdiff_t>(_size / sizeof(keyvalue_t));

        madvise(addr, size * sizeof(keyvalue_t), MADV_WILLNEED);

        if (parallel) {
            std::sort(std::execution::par_unseq, &addr[0], &addr[size]);
        } else {
            std::sort(std::execution::unseq, &addr[0], &addr[size]);
        }

        madvise(addr, size * sizeof(keyvalue_t), MADV_NORMAL);
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
    }
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetUnsortedWriteAccess_mergeBuffers
        (JNIEnv *env, jobject instance, jlong _addr1, jlong _size1, jlong _addr2, jlong _size2, jlong _dstAddr, jlong _dstSize) {
    try {
        assert(_size1 % sizeof(keyvalue_t) == 0);
        assert(_size2 % sizeof(keyvalue_t) == 0);
        assert(_dstSize % sizeof(keyvalue_t) == 0);

        keyvalue_t* addr1 = reinterpret_cast<keyvalue_t*>(_addr1);
        size_t size1 = static_cast<ptrdiff_t>(_size1 / sizeof(keyvalue_t));
        keyvalue_t* addr2 = reinterpret_cast<keyvalue_t*>(_addr2);
        size_t size2 = static_cast<ptrdiff_t>(_size2 / sizeof(keyvalue_t));
        keyvalue_t* dstAddr = reinterpret_cast<keyvalue_t*>(_dstAddr);
        size_t dstSize = static_cast<ptrdiff_t>(_dstSize / sizeof(keyvalue_t));

        assert(size1 + size2 == dstSize);
        assert(addr1 < addr2 ? addr1 + size1 <= addr2 : addr2 + size2 <= addr1);

        madvise(addr1, size1 * sizeof(keyvalue_t), MADV_SEQUENTIAL);
        madvise(addr2, size2 * sizeof(keyvalue_t), MADV_SEQUENTIAL);

        std::merge(&addr1[0], &addr1[size1], &addr2[0], &addr2[size2], &dstAddr[0]);

        madvise(addr1, size1 * sizeof(keyvalue_t), MADV_NORMAL);
        madvise(addr2, size2 * sizeof(keyvalue_t), MADV_NORMAL);
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
    }
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetUnsortedWriteAccess_mergeBuffersInPlace
        (JNIEnv *env, jobject instance, jlong _begin, jlong _middle, jlong _end, jboolean parallel) {
    try {
        assert((_end - _middle) % sizeof(keyvalue_t) == 0);
        assert((_end - _begin) % sizeof(keyvalue_t) == 0);

        keyvalue_t* begin = reinterpret_cast<keyvalue_t*>(_begin);
        keyvalue_t* middle = reinterpret_cast<keyvalue_t*>(_middle);
        keyvalue_t* end = reinterpret_cast<keyvalue_t*>(_end);

        madvise(begin, _end - _begin, MADV_WILLNEED);

        if (parallel) {
            std::inplace_merge(std::execution::par_unseq, begin, middle, end);
        } else {
            std::inplace_merge(std::execution::unseq, begin, middle, end);
        }

        madvise(begin, _end - _begin, MADV_NORMAL);
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
    }
}

JNIEXPORT jboolean JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetUnsortedWriteAccess_isSorted
        (JNIEnv *env, jobject instance, jlong _addr, jlong _size, jboolean parallel) {
    try {
        assert(_size % sizeof(keyvalue_t) == 0);

        keyvalue_t* addr = reinterpret_cast<keyvalue_t*>(_addr);
        ptrdiff_t size = static_cast<ptrdiff_t>(_size / sizeof(keyvalue_t));

        madvise(addr, size * sizeof(keyvalue_t), MADV_SEQUENTIAL);

        bool result;
        if (parallel) {
            result = std::is_sorted(std::execution::par_unseq, &addr[0], &addr[size]);
        } else {
            result = std::is_sorted(std::execution::unseq, &addr[0], &addr[size]);
        }

        madvise(addr, size * sizeof(keyvalue_t), MADV_NORMAL);

        return result;
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
        return false;
    }
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetUnsortedWriteAccess_nWayMerge
        (JNIEnv *env, jobject instance, jlongArray _srcAddrs, jlongArray _srcSizes, jint srcCount, jlong _dstAddr, jlong _dstSize) {
    try {
        assert(_dstSize % sizeof(keyvalue_t) == 0);

        keyvalue_t* dstAddr = reinterpret_cast<keyvalue_t*>(_dstAddr);
        ptrdiff_t dstSize = static_cast<ptrdiff_t>(_dstSize / sizeof(keyvalue_t));

        std::vector<keyvalue_t*> srcAddrs(srcCount);
        std::vector<ptrdiff_t> srcSizes(srcCount);

        env->GetLongArrayRegion(_srcAddrs, 0, srcCount, reinterpret_cast<jlong*>(&srcAddrs[0]));
        env->GetLongArrayRegion(_srcSizes, 0, srcCount, reinterpret_cast<jlong*>(&srcSizes[0]));

        {
            ptrdiff_t totalSrcSize = 0;
            for (jint i = 0; i < srcCount; i++) {
                assert(srcSizes[i] % sizeof(keyvalue_t) == 0);
                srcSizes[i] /= sizeof(keyvalue_t);
                totalSrcSize += srcSizes[i];
            }
            assert(totalSrcSize == dstSize);
        }

        for (jint i = 0; i < srcCount; i++) {
            madvise(srcAddrs[i], srcSizes[i] * sizeof(keyvalue_t), MADV_SEQUENTIAL);
        }
        madvise(dstAddr, dstSize * sizeof(keyvalue_t), MADV_REMOVE);
        madvise(dstAddr, dstSize * sizeof(keyvalue_t), MADV_SEQUENTIAL);

        switch (srcCount) {
            case 0: break;
            case 1: {
                auto src = srcAddrs[0];
                DEBUG_MSG("nWayMerge: using std::copy");
                std::copy(std::execution::par_unseq, &src[0], &src[srcSizes[0]], dstAddr);
                break;
            }
            case 2: {
                auto src0 = srcAddrs[0];
                auto src1 = srcAddrs[1];
                DEBUG_MSG("nWayMerge: using std::merge");
                std::merge(std::execution::par_unseq, &src0[0], &src0[srcSizes[0]], &src1[0], &src1[srcSizes[1]], dstAddr);
                break;
            }
            default: {
                using entry_t = std::pair<keyvalue_t*, keyvalue_t*>;
                if (srcCount <= 32) {
                    DEBUG_MSG("nWayMerge: using linear iteration over std::vector");

                    std::vector<entry_t> srcs;
                    for (jint i = 0; i < srcCount; i++) {
                        srcs.push_back({ srcAddrs[i], &srcAddrs[i][srcSizes[i]] });
                    }

                    ptrdiff_t d = 0;
                    while (d < dstSize) {
                        auto min = std::min_element(srcs.begin(), srcs.end(), [](const entry_t& a, const entry_t& b) { return (*(a.first)) < (*(b.first)); });
                        dstAddr[d++] = *(min->first);

                        if (__builtin_expect(++min->first == min->second, false)) {
                            srcs.erase(min);
                            DEBUG_MSG("completed input buffer, remaining: " << srcs.size());
                        }
                    }
                    assert(srcs.empty());
                } else {
                    DEBUG_MSG("nWayMerge: using std::priority_queue");

                    auto cmp = [](const entry_t& a, const entry_t& b) { return (*(a.first)) > (*(b.first)); };
                    std::priority_queue<entry_t, std::vector<entry_t>, decltype(cmp)> queue(cmp);
                    for (jint i = 0; i < srcCount; i++) {
                        queue.push({ srcAddrs[i], &srcAddrs[i][srcSizes[i]] });
                    }

                    ptrdiff_t d = 0;
                    while (d < dstSize) {
                        entry_t min = queue.top();
                        queue.pop();
                        dstAddr[d++] = *(min.first);

                        if (__builtin_expect(++min.first == min.second, false)) {
                            DEBUG_MSG("completed input buffer, remaining: " << queue.size());
                        } else { [[likely]]
                            queue.push(min);
                        }
                    }
                    assert(queue.empty());
                }
                break;
            }
        }

        for (jint i = 0; i < srcCount; i++) {
            madvise(srcAddrs[i], srcSizes[i] * sizeof(keyvalue_t), MADV_NORMAL);
        }
        madvise(dstAddr, dstSize * sizeof(keyvalue_t), MADV_NORMAL);
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
    }
}

JNIEXPORT jlongArray JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetUnsortedWriteAccess_partitionSortedRange
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
                while (blockEnd + 1 < totalEnd && blockEnd[0].key == blockEnd[1].key) {
                    blockEnd++;
                }
            }

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

JNIEXPORT const keyvalue_t* JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64SetUnsortedWriteAccess_combineAndAppendKey
        (JNIEnv *env, jobject instance, rocksdb::SstFileWriter* writer, const keyvalue_t* begin, const keyvalue_t* end, jboolean merge) {
    try {
        assert(begin < end);
        
        uint64_t key = begin->key;

        //find all entries with the same key
        const keyvalue_t* sequenceEnd = begin;
        do {
            sequenceEnd++;
        } while (sequenceEnd != end && sequenceEnd->key == key);

        std::vector<uint64le> uniqueValues;
        uniqueValues.reserve(sequenceEnd - begin + (merge ? sizeof(operand_t) / sizeof(uint64le) : 0));
        if (merge) {
        }
        for (const keyvalue_t* i = begin; i != sequenceEnd; i++) {
            if (i == begin || i[-1].value != i->value) {
                uniqueValues.push_back(i->value);
            }
        }

        uint64be key_be = key;

        rocksdb::Status status;
        if (merge) {
            operand_t* operand = operand_t::add(uniqueValues);
            status = writer->Merge(
                rocksdb::Slice(reinterpret_cast<const char*>(&key_be), sizeof(uint64be)),
                rocksdb::Slice(reinterpret_cast<const char*>(operand), operand->total_size_with_headers()));
            delete operand;
        } else {
            status = writer->Put(
                rocksdb::Slice(reinterpret_cast<const char*>(&key_be), sizeof(uint64be)),
                rocksdb::Slice(reinterpret_cast<const char*>(&uniqueValues[0]), uniqueValues.size() * sizeof(uint64le)));
        }

        if (!status.ok()) {
            throwRocksdbException(env, status);
        }

        return sequenceEnd;
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
        return nullptr;
    }
}

}
