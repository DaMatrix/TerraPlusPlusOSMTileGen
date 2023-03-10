#include "tpposmtilegen_common.h"
#include "byte_order.h"

#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>

#include <algorithm>
#include <new>
#include <utility>

#include <cassert>
#include <sys/mman.h>

#include <iostream>

#include "UInt64ToBlobMapMergeOperator.h"

namespace {
    class __attribute__((packed)) entry_t {
    public:
        const entry_t* next;
        element_t element;
    };

    static_assert(sizeof(entry_t) == sizeof(uint64_t) * 2 + sizeof(int32_t));
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

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_UInt64ToBlobMapUnsortedWriteAccess_appendKey
        (JNIEnv *env, jobject instance, rocksdb::SstFileWriter* writer, uint64_t key, const entry_t* root) {
    try {
        assert(root != nullptr);

        std::vector<const entry_t*> entries;

        //gather entries into a vector (aka pointer dereference hell)
        for (const entry_t* it = root; it != nullptr; it = it->next) {
            entries.push_back(it);
        }

        //sort elements by subkey
        //std::sort(entries.begin(), entries.end(), [](const entry_t* a, const entry_t* b) { return a->subkey < b->subkey; });
        std::sort(entries.begin(), entries.end(), [](const entry_t* a, const entry_t* b) { return a->element.key < b->element.key; });

        //make sure there are no duplicate subkeys
        for (size_t i = 1; i < entries.size(); i++) {
            //assert(entries[i - 1]->subkey != entries[i]->subkey);
            assert(entries[i - 1]->element.key != entries[i]->element.key);
        }

        //make sure there are no entries containing zeroes
        for (const entry_t* entry : entries) {
            assert(entry->element.value_as_view().find('\0') == std::string_view::npos);
        }

        //compute the total size of the output buffer
        size_t total_size = 0;
        for (const entry_t* entry : entries) {
            assert(entry->element.value_size >= 0);
            total_size += sizeof(element_t) + entry->element.value_size;
        }

        //append values to the buffer
        std::string value_buffer;
        value_buffer.reserve(total_size);
        for (const entry_t* entry : entries) {
            value_buffer.append(entry->element.element_as_view());
        }
        assert(value_buffer.size() == total_size);

        uint64be key_be = key;
        rocksdb::Status status = writer->Put(
                rocksdb::Slice(reinterpret_cast<const char*>(&key_be), sizeof(uint64be)),
                rocksdb::Slice(value_buffer.data(), value_buffer.size()));

        if (!status.ok()) {
            throwRocksdbException(env, status);
            return 0;
        }

        return total_size;
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
        return false;
    }
}

}
