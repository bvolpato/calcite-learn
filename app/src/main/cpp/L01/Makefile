main: libhello.so libhello.jnilib

libhello.so: org_bvolpato_query_jni_L01_HelloWorld.h Hello.cpp
	g++ "-I${JAVA_HOME}/include" "-I${JAVA_HOME}/include/linux" "-I${JAVA_HOME}/include/darwin" -shared -o libhello.so Hello.cpp

libhello.jnilib: org_bvolpato_query_jni_L01_HelloWorld.h Hello.cpp
	g++ "-I${JAVA_HOME}/include" "-I${JAVA_HOME}/include/linux" "-I${JAVA_HOME}/include/darwin" -shared -o libhello.jnilib Hello.cpp

header:
	javac -h . ../../java/org/bvolpato/query/jni/L01_HelloWorld.java

clean:
	rm -rf libhello.so libhello.jnilib
