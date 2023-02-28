#include "tpposmtilegen_common.h"
#include "malloc_extension.h"

#include <string>
#include <cstring>
#include <stdexcept>

#include <sys/mman.h>

constexpr static jint MAX_COPY_SIZE = 32;

template<typename PRIMITIVE>
struct jni_array_range_copied {};

template<>
struct jni_array_range_copied<jbyte> {
private:
    jbyte _data[MAX_COPY_SIZE];
    jint _length;

public:
    explicit inline jni_array_range_copied(JNIEnv* env, jbyteArray array, jint offset, jint length) : _length(length) {
        env->GetByteArrayRegion(array, offset, length, &_data[0]);
    }

    constexpr jbyte* begin() noexcept { return &_data[0]; }
    constexpr const jbyte* begin() const noexcept { return &_data[0]; }
    constexpr jbyte* end() noexcept { return &_data[_length]; }
    constexpr const jbyte* end() const noexcept { return &_data[_length]; }
    constexpr jint length() const noexcept { return _length; }
};

template<typename PRIMITIVE>
struct jni_array_range_referenced {};

template<>
struct jni_array_range_referenced<jbyte> {
private:
    JNIEnv* _env;
    jbyteArray _array;
    jbyte* _data;
    jint _offset;
    jint _length;

public:
    explicit inline jni_array_range_referenced(JNIEnv* env, jbyteArray array, jint offset, jint length) :
        _env(env),
        _array(array), _offset(offset), _length(length),
        _data(env->GetByteArrayElements(array, nullptr)) {
        if (!_data) {
            throw std::runtime_error("GetByteArrayElements returned null!");
        }
    }

    inline ~jni_array_range_referenced() noexcept {
        _env->ReleaseByteArrayElements(_array, _data, JNI_ABORT);
    }

    constexpr jbyte* begin() noexcept { return &_data[0]; }
    constexpr const jbyte* begin() const noexcept { return &_data[0]; }
    constexpr jbyte* end() noexcept { return &_data[_length]; }
    constexpr const jbyte* end() const noexcept { return &_data[_length]; }
    constexpr jint length() const noexcept { return _length; }
};

extern "C" {

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_memcmp0___3BI_3BII
        (JNIEnv *env, jclass cla, jbyteArray s1array, jint offset1, jbyteArray s2array, jint offset2, jint n) {
    if (n <= MAX_COPY_SIZE) {
        jni_array_range_copied<jbyte> s1(env, s1array, offset1, n);
        jni_array_range_copied<jbyte> s2(env, s2array, offset2, n);
        return std::memcmp(s1.begin(), s2.begin(), n);
    } else {
        jni_array_range_referenced<jbyte> s1(env, s1array, offset1, n);
        jni_array_range_referenced<jbyte> s2(env, s2array, offset2, n);
        return std::memcmp(s1.begin(), s2.begin(), n);
    }
}

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_memcmp0___3BIJI
        (JNIEnv *env, jclass cla, jbyteArray s1array, jint offset1, jlong s2, jint n) {
    if (n <= MAX_COPY_SIZE) {
        jni_array_range_copied<jbyte> s1(env, s1array, offset1, n);
        return std::memcmp(s1.begin(), reinterpret_cast<void*>(s2), n);
    } else {
        jni_array_range_referenced<jbyte> s1(env, s1array, offset1, n);
        return std::memcmp(s1.begin(), reinterpret_cast<void*>(s2), n);
    }
}

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_memcmp0__J_3BII
        (JNIEnv *env, jclass cla, jlong s1, jbyteArray s2array, jint offset2, jint n) {
    if (n <= MAX_COPY_SIZE) {
        jni_array_range_copied<jbyte> s2(env, s2array, offset2, n);
        return std::memcmp(reinterpret_cast<void*>(s1), s2.begin(), n);
    } else {
        jni_array_range_referenced<jbyte> s2(env, s2array, offset2, n);
        return std::memcmp(reinterpret_cast<void*>(s1), s2.begin(), n);
    }
}

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_memcmp0__JJJ
        (JNIEnv *env, jclass cla, jlong s1, jlong s2, jlong n) {
    return std::memcmp(reinterpret_cast<void*>(s1), reinterpret_cast<void*>(s2), n);
}

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_madvise0__JJI
        (JNIEnv *env, jclass cla, jlong addr, jlong size, jint usage) {
    static constexpr int USAGE_TABLE[] = { MADV_NORMAL, MADV_RANDOM, MADV_SEQUENTIAL, MADV_WILLNEED, MADV_DONTNEED, MADV_REMOVE };
    return madvise(reinterpret_cast<void*>(addr), size, USAGE_TABLE[usage]);
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_malloc__J
        (JNIEnv *env, jclass cla, jlong size) {
    void* ptr = malloc(size);
    if (ptr == 0) {
        std::string msg = std::to_string(size);
        env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), msg.c_str());
    }
    return reinterpret_cast<jlong>(ptr);
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_realloc__JJ
        (JNIEnv *env, jclass cla, jlong addr, jlong size) {
    void* ptr = realloc(reinterpret_cast<void*>(addr), size);
    if (ptr == 0) {
        std::string msg = std::to_string(size);
        env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), msg.c_str());
    }
    return reinterpret_cast<jlong>(ptr);
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_free__J
        (JNIEnv *env, jclass cla, jlong addr) {
    free(reinterpret_cast<void*>(addr));
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_releaseMemoryToSystem
        (JNIEnv *env, jclass cla) {
    MallocExtension::instance()->ReleaseFreeMemory();
}

}
