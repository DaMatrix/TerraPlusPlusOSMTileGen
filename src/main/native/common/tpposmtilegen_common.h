#ifndef _Included_tpposmtilegen_common_h
#define _Included_tpposmtilegen_common_h

#include <jni.h>
#include <stdexcept>
#include <string>

#ifdef NATIVES_DEBUG
#include <iostream>
#define DEBUG_MSG(msg) std::cout << msg << std::endl;
#else //NATIVES_DEBUG
#define DEBUG_MSG(msg)
#endif //NATIVES_DEBUG

#define EMPTY()
#define LIKELY(x)   __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
//#define LIKELY(x)   x) [[likely]] EMPTY(
//#define UNLIKELY(x) x) [[unlikely]] EMPTY(

#define NORETURN __attribute__((__noreturn__))
#define ALWAYS_INLINE __attribute__((__always_inline__))

jint throwNPE(JNIEnv* env, const char* msg);

jint throwISE(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg, jint err);

jint throwException(JNIEnv* env, const char* msg, jlong err);

class already_thrown_jni_exception {
public:
    already_thrown_jni_exception() noexcept {}
};

class Exception : public std::exception {
private:
    std::string m_text;
public:
    Exception(const std::string& text) : m_text(text) {};
    Exception(std::string&& text) : m_text(text) {};

    const char *what() const noexcept override {
        return m_text.c_str();
    }
};

inline void pushLocalFrame(JNIEnv* env, jint capacity) {
    if (env->PushLocalFrame(capacity) != JNI_OK) {
        throw Exception("can't push local frame");
    }
}

inline void popLocalFrame(JNIEnv* env) noexcept {
    env->PopLocalFrame(nullptr);
}

template<typename T>
inline T popLocalFrame(JNIEnv* env, T result) noexcept {
    return reinterpret_cast<T>(env->PopLocalFrame(result));
}

constexpr static const char* EXCEPTION = "java/lang/Exception";
constexpr static const char* RUNTIME_EXCEPTION = "java/lang/RuntimeException";
constexpr static const char* NULL_POINTER_EXCEPTION = "java/lang/NullPointerException";
constexpr static const char* ILLEGAL_STATE_EXCEPTION = "java/lang/IllegalStateException";
constexpr static const char* OUT_OF_MEMORY_ERROR = "java/lang/OutOfMemoryError";

NORETURN void throwException(JNIEnv* env, const char* class_name, const char* msg, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE());

NORETURN void rethrowException(JNIEnv* env, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE());

ALWAYS_INLINE static void checkException(JNIEnv* env, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE()) {
    if (UNLIKELY(env->ExceptionCheck())) {
        rethrowException(env, function, file, line);
    }
}

jstring toJavaString(JNIEnv* env, const char* str, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE());

ALWAYS_INLINE static jstring toJavaString(JNIEnv* env, const std::string& str, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE()) {
    return toJavaString(env, str.c_str(), function, file, line);
}

jweak makeWeakGlobalReference(JNIEnv* env, jobject obj, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE());

jclass findClassLocal(JNIEnv* env, const char* name, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE());

jclass findClassGlobal(JNIEnv* env, const char* name, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE());

jmethodID findInstanceMethod(JNIEnv* env, jclass cla, const char* name, const char* signature, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE());

jmethodID findStaticMethod(JNIEnv* env, jclass cla, const char* name, const char* signature, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE());

namespace tpp::logging {
    void info(JNIEnv* env, const std::string& msg);
    void info(JNIEnv* env, jobject logger, const std::string& msg);

    void success(JNIEnv* env, const std::string& msg);
    void success(JNIEnv* env, jobject logger, const std::string& msg);

    void warn(JNIEnv* env, const std::string& msg);
    void warn(JNIEnv* env, jobject logger, const std::string& msg);

    void error(JNIEnv* env, const std::string& msg);
    void error(JNIEnv* env, jobject logger, const std::string& msg);

    void fatal(JNIEnv* env, const std::string& msg);
    void fatal(JNIEnv* env, jobject logger, const std::string& msg);

    void alert(JNIEnv* env, const std::string& msg);
    void alert(JNIEnv* env, jobject logger, const std::string& msg);

    void trace(JNIEnv* env, const std::string& msg);
    void trace(JNIEnv* env, jobject logger, const std::string& msg);

    void debug(JNIEnv* env, const std::string& msg);
    void debug(JNIEnv* env, jobject logger, const std::string& msg);
}

namespace tpp::internal {
    void handleException(JNIEnv* env, const std::bad_alloc& e, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE()) noexcept;
    void handleException(JNIEnv* env, const std::runtime_error& e, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE()) noexcept;
    void handleException(JNIEnv* env, const std::exception& e, const char* function = __builtin_FUNCTION(), const char* file = __builtin_FILE(), int line = __builtin_LINE()) noexcept;
}

#define JNI_FUNCTION_HEAD(RETURN_TYPE, NAME, ARGS...) \
extern "C" JNIEXPORT RETURN_TYPE JNICALL Java_ ## NAME (JNIEnv* env, ARGS) noexcept { \
    try {

#define JNI_FUNCTION_TAIL(DEFAULT_RETURN_VALUE...) \
    } catch (const std::bad_alloc& e) { \
        tpp::internal::handleException(env, e); \
    } catch (const std::runtime_error& e) { \
        tpp::internal::handleException(env, e); \
    } catch (const std::exception& e) { \
        tpp::internal::handleException(env, e); \
    } catch (const already_thrown_jni_exception& e) { \
    } \
    return DEFAULT_RETURN_VALUE; \
}

namespace tpp {
    template<typename PTR> requires (std::is_pointer_v<PTR>)
    ALWAYS_INLINE inline PTR jlong_to_ptr(jlong in) noexcept {
        return reinterpret_cast<PTR>(in);
    }

    template<typename PTR> requires (std::is_pointer_v<PTR>)
    ALWAYS_INLINE inline jlong ptr_to_jlong(PTR in) noexcept {
        return static_cast<jlong>(reinterpret_cast<size_t>(in));
    }
}

#endif //_Included_tpposmtilegen_common_h
