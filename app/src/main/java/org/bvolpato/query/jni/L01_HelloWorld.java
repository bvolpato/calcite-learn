package org.bvolpato.query.jni;

public class L01_HelloWorld {

  static {
    System.loadLibrary("hello");
  }

  // Declare an instance native method sayHello() which receives no parameter and returns void
  private native void sayHello(String name);

  public static void main(String[] args) {
    new L01_HelloWorld().sayHello("Frens");
  }
}
