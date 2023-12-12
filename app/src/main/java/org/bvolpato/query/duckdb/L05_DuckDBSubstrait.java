package org.bvolpato.query.duckdb;

import java.nio.ByteBuffer;
import java.sql.*;
import org.apache.arrow.dataset.substrait.AceroSubstraitConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public class L05_DuckDBSubstrait {
  private static final String BASE_DIR = System.getProperty("user.home");

  public static void main(String[] args) throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    // Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    Connection conn = DriverManager.getConnection("jdbc:duckdb:/tmp/sampleduck.db");

    // create a table
    Statement stmt = conn.createStatement();
    stmt.execute("INSTALL substrait; LOAD substrait;");

    String sql =
        """
        SELECT n.n_name, COUNT(C_ACCTBAL) AS BALANCES FROM read_parquet("{HOME}/Downloads/tpch/nation.parquet") n
        JOIN read_parquet("{HOME}/Downloads/tpch/customer.parquet") c
        ON n.n_nationkey = c.c_nationkey
        GROUP BY n.n_name
        ORDER BY BALANCES DESC LIMIT 10
        """
            .replace("{HOME}", BASE_DIR);

    ResultSet plan = stmt.executeQuery("CALL get_substrait('" + sql + "');\n");
    plan.next();

    Blob blob = plan.getBlob(1);

    byte[] bytes = blob.getBytes(0, (int) blob.length());

    //    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    //    System.out.println(gson.toJson(gson.fromJson(json, JsonObject.class)));

    ByteBuffer substraitPlan = ByteBuffer.allocateDirect(bytes.length);
    substraitPlan.put(bytes);

    // run query ????
    try (BufferAllocator allocator = new RootAllocator();
        ArrowReader arrowReader = new AceroSubstraitConsumer(allocator).runQuery(substraitPlan)) {
      while (arrowReader.loadNextBatch()) {
        System.out.print(arrowReader.getVectorSchemaRoot().contentToTSVString());
      }
    }
  }
}
