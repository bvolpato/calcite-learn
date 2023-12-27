package org.bvolpato.query.arrow.adbc;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public class L01_Select {

  public static void main(String[] args) throws Exception {

    final Map<String, Object> parameters = new HashMap<>();
    parameters.put(
        AdbcDriver.PARAM_URL,
        "jdbc:postgresql://localhost:5432/tpch?user=postgres&password=secret");
    try (BufferAllocator allocator = new RootAllocator();
        AdbcDatabase db = new JdbcDriver(allocator).open(parameters);
        AdbcConnection adbcConnection = db.connect();
        AdbcStatement adbcConnectionStatement = adbcConnection.createStatement()) {

      adbcConnectionStatement.setSqlQuery(
          """
SELECT n."N_NAME", ROUND(SUM(s."S_ACCTBAL")/1000) AS balances
FROM nation n
JOIN supplier s ON n."N_NATIONKEY" = s."S_NATIONKEY"
GROUP BY n."N_NAME"
ORDER BY balances DESC;
""");

      AdbcStatement.QueryResult queryResult = adbcConnectionStatement.executeQuery();
      ArrowReader reader = queryResult.getReader();
      while (reader.loadNextBatch()) {
        String s = reader.getVectorSchemaRoot().contentToTSVString();
        System.out.println(s);
      }

      queryResult.close();

    } catch (AdbcException e) {
      // throw
      e.printStackTrace();
    }
  }
}
