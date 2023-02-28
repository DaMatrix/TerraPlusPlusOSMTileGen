#include "tpposmtilegen_common.h"
#include "malloc_extension.h"

#include <new>
#include <string>
#include <string.h>
#include <cstring>
#include <stdexcept>

#include <sys/mman.h>
#include <malloc.h>

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

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_madvise0__JJI
        (JNIEnv *env, jclass cla, jlong addr, jlong size, jint usage) {
    static constexpr int USAGE_TABLE[] = { MADV_NORMAL, MADV_RANDOM, MADV_SEQUENTIAL, MADV_WILLNEED, MADV_DONTNEED, MADV_REMOVE, MADV_HUGEPAGE };
    auto res = madvise(reinterpret_cast<void*>(addr), size, USAGE_TABLE[usage]);
    if (res < 0) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), strerror(errno));
    }
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

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_free__JJ
        (JNIEnv *env, jclass cla, jlong addr, jlong size) {
    //this assumes that malloc and operator delete use the same allocator
    ::operator delete(reinterpret_cast<void*>(addr), static_cast<size_t>(size));
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_releaseMemoryToSystem
        (JNIEnv *env, jclass cla) {
    MallocExtension::instance()->ReleaseFreeMemory();
}

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapProtection_PROT_1EXEC(JNIEnv *env, jclass cla) { return PROT_EXEC; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapProtection_PROT_1READ(JNIEnv *env, jclass cla) { return PROT_READ; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapProtection_PROT_1WRITE(JNIEnv *env, jclass cla) { return PROT_WRITE; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapProtection_PROT_1NONE(JNIEnv *env, jclass cla) { return PROT_NONE; }

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapVisibility_SHARED(JNIEnv *env, jclass cla) { return MAP_SHARED; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapVisibility_SHARED_1VALIDATE(JNIEnv *env, jclass cla) { return MAP_SHARED_VALIDATE; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapVisibility_PRIVATE(JNIEnv *env, jclass cla) { return MAP_PRIVATE; }

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_ANONYMOUS(JNIEnv *env, jclass cla) { return MAP_ANONYMOUS; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_FIXED(JNIEnv *env, jclass cla) { return MAP_FIXED; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_FIXED_1NOREPLACE(JNIEnv *env, jclass cla) { return MAP_FIXED_NOREPLACE; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_GROWSDOWN(JNIEnv *env, jclass cla) { return MAP_GROWSDOWN; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_HUGETLB(JNIEnv *env, jclass cla) { return MAP_HUGETLB; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_HUGE_12MB(JNIEnv *env, jclass cla) { return (21 << MAP_HUGE_SHIFT); }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_HUGE_11GB(JNIEnv *env, jclass cla) { return (30 << MAP_HUGE_SHIFT); }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_LOCKED(JNIEnv *env, jclass cla) { return MAP_LOCKED; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_NORESERVE(JNIEnv *env, jclass cla) { return MAP_NORESERVE; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_POPULATE(JNIEnv *env, jclass cla) { return MAP_POPULATE; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024MapFlags_SYNC(JNIEnv *env, jclass cla) { return MAP_SYNC; }

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_mmap0__JJIIIJ
        (JNIEnv *env, jclass cla, jlong addr, jlong length, jint prot, jint flags, jint fd, jlong offset) {
    void* ptr = mmap(reinterpret_cast<void*>(addr), length, prot, flags, fd, offset);
    if (ptr == MAP_FAILED) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), strerror(errno));
    }
    return reinterpret_cast<jlong>(ptr);
}

JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024RemapFlags_MAYMOVE(JNIEnv *env, jclass cla) { return MREMAP_MAYMOVE; }
JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024RemapFlags_FIXED(JNIEnv *env, jclass cla) { return MREMAP_FIXED; }
//JNIEXPORT jint JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_00024RemapFlags_DONTUNMAP(JNIEnv *env, jclass cla) { return MREMAP_DONTUNMAP; }

JNIEXPORT jlong JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_mremap0__JJJIJ
        (JNIEnv *env, jclass cla, jlong old_address, jlong old_size, jlong new_size, jint flags, jlong new_address) {
    void* ptr = mremap(reinterpret_cast<void*>(old_address), old_size, new_size, flags, new_address);
    if (ptr == MAP_FAILED) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), strerror(errno));
    }
    return reinterpret_cast<jlong>(ptr);
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_mprotect0__JJI
        (JNIEnv *env, jclass cla, jlong addr, jlong length, jint prot) {
    auto res = mprotect(reinterpret_cast<void*>(addr), length, prot);
    if (res < 0) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), strerror(errno));
    }
}

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_Memory_munmap__JJ
        (JNIEnv *env, jclass cla, jlong addr, jlong length) {
    auto res = munmap(reinterpret_cast<void*>(addr), length);
    if (res < 0) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), strerror(errno));
    }
}

}
