#include "tpposmtilegen_common.h"

#include <lib-rocksdb/include/rocksdb/db.h>
#include <lib-rocksdb/include/rocksdb/iterator.h>
#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>
#include <lib-rocksdb/include/rocksdb/write_batch.h>

#include <cassert>
#include <stdexcept>
#include <vector>

static void throwOutOfMemory(JNIEnv* env, const char* msg = "") noexcept {
    if (!env->ExceptionCheck()) {
        env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), msg);
    }
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

static jmethodID method_keyValueSlice_set;

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_init
        (JNIEnv *env, jclass cla) {
    if (jclass keyValueSlice = env->FindClass("net/daporkchop/tpposmtilegen/natives/NativeRocksHelper$KeyValueSlice"); keyValueSlice) {
        method_keyValueSlice_set = env->GetMethodID(keyValueSlice, "set", "(JJJJ)V");
    }
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_writeBatchHeaderSize0
        (JNIEnv *env, jclass cla) {
    rocksdb::WriteBatch write_batch;
    assert(write_batch.Count() == 0);
    return static_cast<jlong>(write_batch.GetDataSize());
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_getKeyAndValueAsView0
        (JNIEnv *env, jclass cla, jlong handle, jobject slice) {
    auto* iterator = reinterpret_cast<rocksdb::Iterator*>(handle);
    rocksdb::Slice key = iterator->key();
    rocksdb::Slice value = iterator->value();
    env->CallVoidMethod(slice, method_keyValueSlice_set,
            reinterpret_cast<jlong>(key.data()), static_cast<jlong>(key.size()),
            reinterpret_cast<jlong>(value.data()), static_cast<jlong>(value.size()));
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_writeBatchMerge0
        (JNIEnv *env, jclass cla, jlong handle, jlong cf_handle, jlong keyAddr, jint keySize, jlong valueAddr, jint valueSize) {
    auto* batch = reinterpret_cast<rocksdb::WriteBatch*>(handle);
    auto* column_family = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(cf_handle);
    rocksdb::Slice key(reinterpret_cast<const char*>(keyAddr), keySize);
    rocksdb::Slice value(reinterpret_cast<const char*>(valueAddr), valueSize);
    if (auto status = batch->Merge(column_family, key, value); UNLIKELY(!status.ok())) {
        throwRocksdbException(env, status);
    }
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_sstFileWriterMerge0
        (JNIEnv *env, jclass cla, jlong handle, jlong keyAddr, jint keySize, jlong valueAddr, jint valueSize) {
    auto* writer = reinterpret_cast<rocksdb::SstFileWriter*>(handle);
    rocksdb::Slice key(reinterpret_cast<const char*>(keyAddr), keySize);
    rocksdb::Slice value(reinterpret_cast<const char*>(valueAddr), valueSize);
    if (auto status = writer->Merge(key, value); UNLIKELY(!status.ok())) {
        throwRocksdbException(env, status);
    }
}

static_assert(sizeof(rocksdb::Slice) >= alignof(rocksdb::Slice));
static_assert(alignof(const void*) >= alignof(rocksdb::Slice));
JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_00024OffHeapSliceArray_SIZE(JNIEnv *env, jclass cla) { return sizeof(rocksdb::Slice); }
JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_00024OffHeapSliceArray_DATA_1OFFSET(JNIEnv *env, jclass cla) { return offsetof(rocksdb::Slice, data_); }
JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_00024OffHeapSliceArray_DATA_1SIZE(JNIEnv *env, jclass cla) { return sizeof(rocksdb::Slice().data_); }
JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_00024OffHeapSliceArray_SIZE_1OFFSET(JNIEnv *env, jclass cla) { return offsetof(rocksdb::Slice, size_); }
JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_00024OffHeapSliceArray_SIZE_1SIZE(JNIEnv *env, jclass cla) { return sizeof(rocksdb::Slice().size_); }

JNIEXPORT jobjectArray JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_multiGetToArrays0__JJJIJZ
        (JNIEnv *env, jclass cla, jlong dbHandle, jlong optionsHandle, jlong columnFamilyHandle, jint numKeys, jlong keySlicesAddr, jboolean sortedInput) {
    auto* db = reinterpret_cast<rocksdb::DB*>(dbHandle);
    auto* options = reinterpret_cast<const rocksdb::ReadOptions*>(optionsHandle);
    auto* column_family = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(columnFamilyHandle);
    auto* keys = reinterpret_cast<const rocksdb::Slice*>(keySlicesAddr);

    try {
        std::vector<rocksdb::PinnableSlice> values(numKeys);
        std::vector<rocksdb::Status> statuses(numKeys);

        db->MultiGet(*options, column_family, numKeys, keys, values.data(), statuses.data());

        for (const auto& status : statuses) {
            if (UNLIKELY(!status.ok())) {
                throwRocksdbException(env, status);
                return nullptr;
            }
        }

        jobjectArray results = env->NewObjectArray(numKeys, env->FindClass("[B"), nullptr);
        if (UNLIKELY(results == nullptr)) return nullptr;

        for (decltype(numKeys) i = 0; i < numKeys; i++) {
            jint size = static_cast<jint>(values[i].size());
            jbyteArray arr = env->NewByteArray(size);
            if (UNLIKELY(arr == nullptr)) return nullptr;

            env->SetObjectArrayElement(results, i, arr);
            env->SetByteArrayRegion(arr, 0, size, reinterpret_cast<const jbyte*>(values[i].data()));
        }

        return results;
    } catch (const std::bad_alloc& e) {
        throwOutOfMemory(env, e);
        return nullptr;
    }
}

}
