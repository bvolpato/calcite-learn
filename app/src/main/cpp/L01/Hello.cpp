// Save as "HelloJNI.c"
#include <jni.h>        // JNI header provided by JDK
#include <stdio.h>      // C Standard IO Header
#include "Hello.h"   // Generated

// Implementation of the native method sayHello()
JNIEXPORT void JNICALL Java_L01_1HelloWorld_sayHello
  (JNIEnv *, jobject) {
   printf("Hello JNI!\n");
   return;
}