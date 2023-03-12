#include "tpposmtilegen_common.h"

#include <atomic>
#include <cassert>

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

static std::string exception_location_to_string(const char* function, const char* file, int line) {
    return std::string("at ") + function + " (" + file + ":" + std::to_string(line) + ")";
}

static void throwExceptionOnlyJni(JNIEnv* env, const char* class_name, const char* msg, const char* function, const char* file, int line) {
    jclass clazz = findClassLocal(env, class_name);

    std::string full_msg = exception_location_to_string(function, file, line) + ": '" + msg + "'";
    jint status = env->ThrowNew(clazz, full_msg.c_str());
    if (LIKELY(status == 0)) {
        return;
    }

    std::string fatal_error_msg = exception_location_to_string(function, file, line)
            + ": failed to throw new '" + class_name + "' with message '" + msg + "' because env->ThrowNew() returned " + std::to_string(status);
    env->FatalError(fatal_error_msg.c_str());
    __builtin_unreachable();
}

NORETURN void throwException(JNIEnv* env, const char* class_name, const char* msg, const char* function, const char* file, int line) {
    throwExceptionOnlyJni(env, class_name, msg, function, file, line);
    throw already_thrown_jni_exception();
}

void tpp::internal::handleException(JNIEnv* env, const std::bad_alloc& e, const char* function, const char* file, int line) noexcept {
    throwExceptionOnlyJni(env, OUT_OF_MEMORY_ERROR, e.what(), function, file, line);
}

void tpp::internal::handleException(JNIEnv* env, const std::runtime_error& e, const char* function, const char* file, int line) noexcept {
    throwExceptionOnlyJni(env, RUNTIME_EXCEPTION, e.what(), function, file, line);
}

void tpp::internal::handleException(JNIEnv* env, const std::exception& e, const char* function, const char* file, int line) noexcept {
    throwExceptionOnlyJni(env, EXCEPTION, e.what(), function, file, line);
}

NORETURN void rethrowException(JNIEnv* env, const char* function, const char* file, int line) {
    //jthrowable exception = env->ExceptionOccurred();
    if (UNLIKELY(!env->ExceptionCheck())) {
        std::string msg = std::string("no exception occurred?!? ") + exception_location_to_string(function, file, line);
        throwException(env, ILLEGAL_STATE_EXCEPTION, msg.c_str(), function, file, line);
    }
    throw already_thrown_jni_exception();
}

jstring toJavaString(JNIEnv* env, const char* str, const char* function, const char* file, int line) {
    jstring string = env->NewStringUTF(str);
    if (UNLIKELY(string == nullptr)) rethrowException(env); //OutOfMemoryError has already been thrown
    return string;
}

jobject makeWeakGlobalReference(JNIEnv* env, jobject obj, const char* function, const char* file, int line) {
    assert(obj != nullptr);

    jweak weak = env->NewWeakGlobalRef(obj);
    if (UNLIKELY(weak == nullptr)) rethrowException(env); //OutOfMemoryError has already been thrown
    return weak;
}

jclass findClassLocal(JNIEnv* env, const char* name, const char* function, const char* file, int line) {
    jclass clazz = env->FindClass(name);
    if (UNLIKELY(clazz == nullptr)) rethrowException(env); //NoClassDefFoundError has already been thrown
    return clazz;
}

jclass findClassGlobal(JNIEnv* env, const char* name, const char* function, const char* file, int line) {
    jclass clazz = reinterpret_cast<jclass>(env->NewGlobalRef(findClassLocal(env, name)));
    assert(clazz != nullptr);
    return clazz;
}

jmethodID findInstanceMethod(JNIEnv* env, jclass cla, const char* name, const char* signature, const char* function, const char* file, int line) {
    jmethodID method = env->GetMethodID(cla, name, signature);
    if (UNLIKELY(method == nullptr)) rethrowException(env); //NoSuchMethodError has already been thrown
    return method;
}

template<typename RET> struct method_caller {};

template<> struct method_caller<jobject> {
    static jobject callInstanceMethod(JNIEnv* env, jobject instance, jmethodID method, auto... args) {
        auto ret = env->CallObjectMethod(instance, method, args...);
        checkException(env);
        return ret;
    }
};

template<typename RET> RET callInstanceMethod(JNIEnv* env, jobject instance, jmethodID method, auto... args) { return method_caller<RET>::callInstanceMethod(env, instance, method, args...); }

static struct {
    jweak global_logger_instance;
    jmethodID method_info;
    jmethodID method_success;
    jmethodID method_warn;
    jmethodID method_error;
    jmethodID method_fatal;
    jmethodID method_alert;
    jmethodID method_trace;
    jmethodID method_debug;
} logger_data;

extern "C" JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_Natives_init
        (JNIEnv* env, jclass cla, jobject logger) {
    jclass c_logger = findClassLocal(env, "net/daporkchop/lib/logging/Logger");
    logger_data.global_logger_instance = makeWeakGlobalReference(env, logger);
    logger_data.method_info = findInstanceMethod(env, c_logger, "info", "(Ljava/lang/String;)Lnet/daporkchop/lib/logging/Logger;");
    logger_data.method_success = findInstanceMethod(env, c_logger, "success", "(Ljava/lang/String;)Lnet/daporkchop/lib/logging/Logger;");
    logger_data.method_warn = findInstanceMethod(env, c_logger, "warn", "(Ljava/lang/String;)Lnet/daporkchop/lib/logging/Logger;");
    logger_data.method_error = findInstanceMethod(env, c_logger, "error", "(Ljava/lang/String;)Lnet/daporkchop/lib/logging/Logger;");
    logger_data.method_fatal = findInstanceMethod(env, c_logger, "fatal", "(Ljava/lang/String;)Lnet/daporkchop/lib/logging/Logger;");
    logger_data.method_alert = findInstanceMethod(env, c_logger, "alert", "(Ljava/lang/String;)Lnet/daporkchop/lib/logging/Logger;");
    logger_data.method_trace = findInstanceMethod(env, c_logger, "trace", "(Ljava/lang/String;)Lnet/daporkchop/lib/logging/Logger;");
    logger_data.method_debug = findInstanceMethod(env, c_logger, "debug", "(Ljava/lang/String;)Lnet/daporkchop/lib/logging/Logger;");
}

void tpp::logging::info(JNIEnv* env, const std::string& msg) { callInstanceMethod<jobject>(env, logger_data.global_logger_instance, logger_data.method_info, toJavaString(env, msg)); }
void tpp::logging::info(JNIEnv* env, jobject logger, const std::string& msg) { callInstanceMethod<jobject>(env, logger, logger_data.method_info, toJavaString(env, msg)); }
void tpp::logging::success(JNIEnv* env, const std::string& msg) { callInstanceMethod<jobject>(env, logger_data.global_logger_instance, logger_data.method_success, toJavaString(env, msg)); }
void tpp::logging::success(JNIEnv* env, jobject logger, const std::string& msg) { callInstanceMethod<jobject>(env, logger, logger_data.method_success, toJavaString(env, msg)); }
void tpp::logging::warn(JNIEnv* env, const std::string& msg) { callInstanceMethod<jobject>(env, logger_data.global_logger_instance, logger_data.method_warn, toJavaString(env, msg)); }
void tpp::logging::warn(JNIEnv* env, jobject logger, const std::string& msg) { callInstanceMethod<jobject>(env, logger, logger_data.method_warn, toJavaString(env, msg)); }
void tpp::logging::error(JNIEnv* env, const std::string& msg) { callInstanceMethod<jobject>(env, logger_data.global_logger_instance, logger_data.method_error, toJavaString(env, msg)); }
void tpp::logging::error(JNIEnv* env, jobject logger, const std::string& msg) { callInstanceMethod<jobject>(env, logger, logger_data.method_error, toJavaString(env, msg)); }
void tpp::logging::fatal(JNIEnv* env, const std::string& msg) { callInstanceMethod<jobject>(env, logger_data.global_logger_instance, logger_data.method_fatal, toJavaString(env, msg)); }
void tpp::logging::fatal(JNIEnv* env, jobject logger, const std::string& msg) { callInstanceMethod<jobject>(env, logger, logger_data.method_fatal, toJavaString(env, msg)); }
void tpp::logging::alert(JNIEnv* env, const std::string& msg) { callInstanceMethod<jobject>(env, logger_data.global_logger_instance, logger_data.method_alert, toJavaString(env, msg)); }
void tpp::logging::alert(JNIEnv* env, jobject logger, const std::string& msg) { callInstanceMethod<jobject>(env, logger, logger_data.method_alert, toJavaString(env, msg)); }
void tpp::logging::trace(JNIEnv* env, const std::string& msg) { callInstanceMethod<jobject>(env, logger_data.global_logger_instance, logger_data.method_trace, toJavaString(env, msg)); }
void tpp::logging::trace(JNIEnv* env, jobject logger, const std::string& msg) { callInstanceMethod<jobject>(env, logger, logger_data.method_trace, toJavaString(env, msg)); }
void tpp::logging::debug(JNIEnv* env, const std::string& msg) { callInstanceMethod<jobject>(env, logger_data.global_logger_instance, logger_data.method_debug, toJavaString(env, msg)); }
void tpp::logging::debug(JNIEnv* env, jobject logger, const std::string& msg) { callInstanceMethod<jobject>(env, logger, logger_data.method_debug, toJavaString(env, msg)); }
