package org.bvolpato.query.arrow;

import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

public class L05_VectorSearch {

  public static void main(String[] args) throws Exception {

    System.out.println("=============================\nintVector:");

    try (BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("intVector", allocator);
        IntVector search = new IntVector("search", allocator)) {

      int totalNums = 600_000_000;
      for (int i = 0; i < totalNums; i++) {
        intVector.setSafe(i, i + 1);
      }
      intVector.setValueCount(totalNums);

      search.setSafe(0, 575_050_050);
      search.setValueCount(1);

      VectorValueComparator<IntVector> valueComparator =
          DefaultVectorComparators.createDefaultComparator(intVector);
      valueComparator.attachVector(intVector);

      System.out.println("Searching...");

      {
        long start = System.currentTimeMillis();
        int res = VectorSearcher.linearSearch(intVector, valueComparator, search, 0);
        System.out.println("Linear finished in " + (System.currentTimeMillis() - start) + "ms.");
        System.out.println("Found index: " + res);
      }

      {
        long binaryStart = System.currentTimeMillis();
        int res = VectorSearcher.binarySearch(intVector, valueComparator, search, 0);
        System.out.println(
            "Binary finished in " + (System.currentTimeMillis() - binaryStart) + "ms.");
        System.out.println("Found index: " + res);
      }

      //      {
      //        long start = System.currentTimeMillis();
      //        ExecutorService threadPool = Executors.newFixedThreadPool(16);
      //        ParallelSearcher<IntVector> searcher = new ParallelSearcher<>(intVector, threadPool,
      // 16);
      //        int res = searcher.search(search, 0, valueComparator);
      //        System.out.println("Parallel finished in " + (System.currentTimeMillis() - start) +
      // "ms.");
      //        System.out.println("Found index (parallel): " + res);
      //        threadPool.shutdown();
      //      }
    }
  }
}
