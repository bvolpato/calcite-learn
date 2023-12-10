package org.bvolpato.query.arrow;

import com.google.common.collect.ImmutableList;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.proto.Plan;
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
import org.apache.arrow.dataset.substrait.AceroSubstraitConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.calcite.sql.parser.SqlParseException;

public class L08_SubstraitJoin {

  public static void main(String[] args) {
    String uriNation = "file:///home/bvolpato/Downloads/tpch/nation.parquet";
    String uriCustomer = "file:///home/bvolpato/Downloads/tpch/customer.parquet";

    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactoryNation =
            new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uriNation);
        Dataset datasetNation = datasetFactoryNation.finish();
        Scanner scannerNation = datasetNation.newScan(options);
        ArrowReader readerNation = scannerNation.scanBatches();
        DatasetFactory datasetFactoryCustomer =
            new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uriCustomer);
        Dataset datasetCustomer = datasetFactoryCustomer.finish();
        Scanner scannerCustomer = datasetCustomer.newScan(options);
        ArrowReader readerCustomer = scannerCustomer.scanBatches()) {

      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", readerNation);
      mapTableToArrowReader.put("CUSTOMER", readerCustomer);

      // get binary plan
      Plan plan = queryTableNationJoinCustomer();
      System.out.println("Plan: " + plan);

      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
      substraitPlan.put(plan.toByteArray());

      // run query
      try (ArrowReader arrowReader =
          new AceroSubstraitConsumer(allocator).runQuery(substraitPlan, mapTableToArrowReader)) {
        while (arrowReader.loadNextBatch()) {
          System.out.print(arrowReader.getVectorSchemaRoot().contentToTSVString());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static Plan queryTableNationJoinCustomer() throws SqlParseException {
    String sql =
        """
                SELECT n.n_name, AVG(C_ACCTBAL) AS BALANCES FROM NATION n JOIN CUSTOMER c
                ON n.n_nationkey = c.c_nationkey
                GROUP BY n.n_name
                ORDER BY BALANCES DESC LIMIT 10
                """;
    String nation =
        """
                CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL,
                N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))
                """;
    String customer =
        """
                CREATE TABLE CUSTOMER (C_CUSTKEY BIGINT NOT NULL,
                C_NAME VARCHAR(25), C_ADDRESS VARCHAR(40), C_NATIONKEY BIGINT NOT NULL,
                C_PHONE CHAR(15), C_ACCTBAL DECIMAL, C_MKTSEGMENT CHAR(10),
                C_COMMENT VARCHAR(117) )
                """;

    SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    return sqlToSubstrait.execute(sql, ImmutableList.of(nation, customer));
  }
}
