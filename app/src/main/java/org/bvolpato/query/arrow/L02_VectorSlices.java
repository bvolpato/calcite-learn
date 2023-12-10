package org.bvolpato.query.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.util.TransferPair;

public class L02_VectorSlices {

  public static void main(String[] args) {

    System.out.println("=============================\nintVector:");

    try (BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("intVector", allocator)) {

      for (int i = 0; i < 10; i++) {
        intVector.setSafe(i, i);
      }
      intVector.setValueCount(10);

      System.out.println(intVector);

      TransferPair tp = intVector.getTransferPair(allocator);
      tp.splitAndTransfer(0, 5);

      try (IntVector slice = (IntVector) tp.getTo()) {
        System.out.println(slice);
      }

      tp.splitAndTransfer(5, 5);
      try (IntVector slice = (IntVector) tp.getTo()) {
        System.out.println(slice);
      }
    }
  }
}
