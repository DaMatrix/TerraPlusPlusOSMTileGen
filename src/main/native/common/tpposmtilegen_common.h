#ifndef _Included_tpposmtilegen_common_h
#define _Included_tpposmtilegen_common_h

#include <jni.h>

#include "osmium/area/assembler.hpp"

jint throwNPE(JNIEnv* env, const char* msg);

jint throwISE(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg, jint err);

jint throwException(JNIEnv* env, const char* msg, jlong err);

#endif //_Included_tpposmtilegen_common_h
