package org.bvolpato.query.arrow;

import org.apache.arrow.algorithm.sort.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

public class L06_VectorSort {

  public static void main(String[] args) throws Exception {

    System.out.println("=============================\nintVector:");

    try (BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("intVector", allocator)) {

      int totalNums = 1000;
      for (int i = 0; i < totalNums * 100; i++) {
        intVector.setSafe((int) (Math.random() * totalNums), i % totalNums);
      }
      intVector.setValueCount(totalNums);
      System.out.println(intVector);

      //      OutOfPlaceVectorSorter<IntVector> sorterOutOfPlaceSorter =
      //          new FixedWidthOutOfPlaceVectorSorter<>();
      //      VectorValueComparator<IntVector> comparatorOutOfPlaceSorter =
      //          DefaultVectorComparators.createDefaultComparator(intVector);
      //      intVectorSorted.allocateNew(intVector.getValueCount());
      //      intVectorSorted.setValueCount(intVector.getValueCount());
      //      sorterOutOfPlaceSorter.sortOutOfPlace(intVector, intVectorSorted,
      // comparatorOutOfPlaceSorter);

      FixedWidthInPlaceVectorSorter<IntVector> sorter = new FixedWidthInPlaceVectorSorter<>();
      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(intVector);
      sorter.sortInPlace(intVector, comparator);

      System.out.println(intVector);
    }
  }
}
