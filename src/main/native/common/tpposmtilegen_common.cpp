#include "tpposmtilegen_common.h"

jint throwNPE(JNIEnv* env, const char* msg)  {
    jclass clazz = env->FindClass("java/lang/NullPointerException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V"),
        env->NewStringUTF(msg)
    ));
}

jint throwISE(JNIEnv* env, const char* msg)  {
    jclass clazz = env->FindClass("java/lang/IllegalStateException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V"),
        env->NewStringUTF(msg)
    ));
}

jint throwException(JNIEnv* env, const char* msg)  {
    jclass clazz = env->FindClass("net/daporkchop/lib/natives/NativeException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V"),
        env->NewStringUTF(msg)
    ));
}

jint throwException(JNIEnv* env, const char* msg, jint err)  {
    jclass clazz = env->FindClass("net/daporkchop/lib/natives/NativeException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;I)V"),
        env->NewStringUTF(msg),
        err
    ));
}

jint throwException(JNIEnv* env, const char* msg, jlong err)  {
    jclass clazz = env->FindClass("net/daporkchop/lib/natives/NativeException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;J)V"),
        env->NewStringUTF(msg),
        err
    ));
}
