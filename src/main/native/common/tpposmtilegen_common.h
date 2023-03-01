#ifndef _Included_tpposmtilegen_common_h
#define _Included_tpposmtilegen_common_h

#include <jni.h>
#include <exception>
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

jint throwNPE(JNIEnv* env, const char* msg);

jint throwISE(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg, jint err);

jint throwException(JNIEnv* env, const char* msg, jlong err);

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

#endif //_Included_tpposmtilegen_common_h
