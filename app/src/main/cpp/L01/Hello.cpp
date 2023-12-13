// Save as "HelloJNI.c"
#include <jni.h>        // JNI header provided by JDK
#include <stdio.h>      // C Standard IO Header
#include <string>
#include "org_bvolpato_query_jni_L01_HelloWorld.h"   // Generated

// Implementation of the native method sayHello()
JNIEXPORT void JNICALL Java_org_bvolpato_query_jni_L01_1HelloWorld_sayHello
  (JNIEnv * env, jobject obj, jstring nameParam) {

   // Convert to a proper string
   jboolean isCopy;
   const char *convertedValue = (env)->GetStringUTFChars(nameParam, &isCopy);
   std::string convStr = convertedValue;

   printf("Hello, %s!\n", convStr.c_str());

   return;
}