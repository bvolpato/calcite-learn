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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.calcite.sql.parser.SqlParseException;

public class L07_SubstraitSingle {

  private static final String BASE_DIR = System.getProperty("user.home");

  public static void main(String[] args) {
    String uri = "file://" + BASE_DIR + "/Downloads/tpch/nation.parquet";
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory =
            new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()) {

      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", reader);

      // get binary plan
      Plan plan = getPlan();

      System.out.println("Plan: " + plan);

      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
      substraitPlan.put(plan.toByteArray());

      // run query
      try (ArrowReader arrowReader =
          new AceroSubstraitConsumer(allocator).runQuery(substraitPlan, mapTableToArrowReader)) {
        while (arrowReader.loadNextBatch()) {
          VectorSchemaRoot vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
          System.out.println(vectorSchemaRoot.contentToTSVString());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static Plan getPlan() throws SqlParseException {
    String sql =
        """
                SELECT * from nation n where n.n_regionkey = 1
                """;
    String nation =
        """
                CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25),
                N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))
                """;

    SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    return sqlToSubstrait.execute(sql, ImmutableList.of(nation));
  }
}
