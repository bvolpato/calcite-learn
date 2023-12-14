/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bvolpato.query.jelox;

import com.google.common.collect.ImmutableList;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

public class L01_SimplePlan {

  private static final String BASE_DIR = System.getProperty("user.home");

  public static void main(String[] args) throws Exception {

    String uriNation = "file://" + BASE_DIR + "/Downloads/tpch/nation.parquet";
    String uriCustomer = "file://" + BASE_DIR + "/Downloads/tpch/customer.parquet";

    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactoryNation =
            new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uriNation);
        Dataset datasetNation = datasetFactoryNation.finish();
        Scanner scannerNation = datasetNation.newScan(options);
        ArrowReader readerNation = scannerNation.scanBatches()) {

      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", readerNation);

      String sql = """
                SELECT n_nationkey, n_name FROM nation
                """;
      String nation =
          """
                      CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL,
                      N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))
                      """;

      //      while (readerNation.loadNextBatch()) {
      //        VectorSchemaRoot vectorSchemaRoot = readerNation.getVectorSchemaRoot();
      //        System.out.println("nation me " + vectorSchemaRoot.contentToTSVString());
      //      }

      SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
      Plan plan = sqlToSubstrait.execute(sql, ImmutableList.of(nation));

      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
      substraitPlan.put(plan.toByteArray());

      System.out.println(plan);

      for (PlanRel planRel : plan.getRelationsList()) {
        System.out.println("PlanRel: " + planRel);
      }

      //      try (ArrowReader arrowReader =
      //          new AceroSubstraitConsumer(allocator).runQuery(substraitPlan,
      // mapTableToArrowReader)) {
      //        while (arrowReader.loadNextBatch()) {
      //          System.out.print(arrowReader.getVectorSchemaRoot().contentToTSVString());
      //        }
      //      }

      try (VectorSchemaRoot arrowReader =
          new JeloxSubstraitConsumer(allocator).execute(plan, mapTableToArrowReader)) {
        System.out.print(arrowReader.contentToTSVString());
      }
    }
  }
}
