#include "tpposmtilegen_common.h"

#include <lib-rocksdb/include/rocksdb/iterator.h>
#include <lib-rocksdb/include/rocksdb/sst_file_writer.h>
#include <lib-rocksdb/include/rocksdb/write_batch.h>

#include <cassert>

static void throwRocksdbException(JNIEnv* env, const rocksdb::Status& status) noexcept {
    if (!env->ExceptionCheck()) {
        std::string msg = status.ToString();
        env->ThrowNew(env->FindClass("org/rocksdb/RocksDBException"), msg.c_str());
    }
}

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_init
        (JNIEnv *env, jclass cla) {
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_NativeRocksHelper_writeBatchHeaderSize0
        (JNIEnv *env, jclass cla) {
    rocksdb::WriteBatch write_batch;
    //return static_cast<jint>(WriteBatchInternal::kHeader);
    assert(write_batch.Count() == 0);
    return static_cast<jlong>(write_batch.GetDataSize());
}

}
