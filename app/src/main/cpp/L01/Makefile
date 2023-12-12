libhello.so: Hello.h Hello.cpp
	gcc -I/usr/lib/jvm/java-17-openjdk-amd64/include -I/usr/lib/jvm/java-17-openjdk-amd64/include/linux -shared -o libhello.so Hello.cpp

clean:
	rm -rf libhello.so
