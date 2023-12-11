package org.bvolpato.query.duckdb;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;
import org.duckdb.DuckDBResultSet;

public class L03_DuckDBQueryLarge {
  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    Class.forName("org.duckdb.DuckDBDriver");

    Properties readOnlyProp = new Properties();
    readOnlyProp.setProperty(DuckDBDriver.DUCKDB_READONLY_PROPERTY, "true");

    try (DuckDBConnection conn =
            (DuckDBConnection)
                DriverManager.getConnection("jdbc:duckdb:/tmp/sampleduck.db", readOnlyProp);
        Statement stmt = conn.createStatement();
        DuckDBResultSet res =
            (DuckDBResultSet)
                stmt.executeQuery(
                    """
                    SELECT l_orderkey, l_linenumber FROM lineitem LIMIT 10000
                    """);
        RootAllocator allocator = new RootAllocator()) {

      try (ArrowReader reader = (ArrowReader) res.arrowExportStream(allocator, 2 << 20)) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
          System.out.println(schemaRoot.contentToTSVString());
        }
      }
    }
  }
}
