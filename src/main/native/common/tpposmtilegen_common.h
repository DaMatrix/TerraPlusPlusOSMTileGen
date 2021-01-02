#ifndef _Included_tpposmtilegen_common_h
#define _Included_tpposmtilegen_common_h

#include <jni.h>
#include <exception>
#include <string>

jint throwNPE(JNIEnv* env, const char* msg);

jint throwISE(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg, jint err);

jint throwException(JNIEnv* env, const char* msg, jlong err);

class Exception : public std::exception {
private:
    std::string m_text;
public:
    Exception(std::string text) : m_text(std::move(text)) {};

    Exception(std::string &text) : m_text(text) {};

    const char *what() const noexcept override {
        return m_text.c_str();
    }
};

#endif //_Included_tpposmtilegen_common_h
