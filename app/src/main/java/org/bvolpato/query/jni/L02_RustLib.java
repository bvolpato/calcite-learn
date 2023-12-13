package org.bvolpato.query.jni;

import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

public class L02_RustLib {

  static {
    System.loadLibrary("arrow_learn");
  }

  private native int add(int num1, int num2);

  private native long multiply(long memoryAddress, int size);

  private native long sum(long memoryAddress, int size);

  public static void main(String[] args) throws IOException {
    System.out.println("Sum in Rust is: " + new L02_RustLib().add(2, 3));
    System.out.println("=============================\nintVector:");

    try (BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("intVector", allocator)) {

      for (int i = 1; i <= 8; i++) {
        intVector.setSafe(i - 1, i);
      }
      intVector.setValueCount(8);

      long multiply =
          new L02_RustLib()
              .multiply(intVector.getDataBuffer().memoryAddress(), intVector.getValueCount());
      System.out.println("fac(8): " + multiply);
    }

    System.out.println();

    try (BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("intVector", allocator)) {

      for (int i = 0; i < 100_000_000; i++) {
        intVector.setSafe(i, i + 1);
      }
      intVector.setValueCount(100_000_000);

      long startLocal = System.currentTimeMillis();
      long sumLocal = 0;
      for (int i = 0; i < intVector.getValueCount(); i++) {
        sumLocal += intVector.get(i);
      }
      System.out.println(
          "Sum of all ints: "
              + sumLocal
              + " (took "
              + (System.currentTimeMillis() - startLocal)
              + "ms in local)");

      long startJni = System.currentTimeMillis();
      long sum =
          new L02_RustLib()
              .sum(intVector.getDataBuffer().memoryAddress(), intVector.getValueCount());
      System.out.println(
          "Sum of all ints: "
              + sum
              + " (took "
              + (System.currentTimeMillis() - startJni)
              + "ms in JNI)");
    }
  }
}
