package org.bvolpato.query.duckdb;

import java.sql.*;
import org.duckdb.DuckDBConnection;

public class L02_DuckDBQueryAgg {
  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.duckdb.DuckDBDriver");

    DuckDBConnection conn =
        (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:/tmp/sampleduck.db");

    // create a table
    Statement stmt = conn.createStatement();

    String sql =
        """
                    SELECT n.n_name, ROUND(AVG(C_ACCTBAL), 2) AS BALANCES FROM NATION n JOIN CUSTOMER c
                    ON n.n_nationkey = c.c_nationkey
                    GROUP BY n.n_name
                    ORDER BY BALANCES DESC LIMIT 10
                    """;

    ResultSet resultSet = stmt.executeQuery(sql);
    ResultSetMetaData metaData = resultSet.getMetaData();

    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      System.out.print(metaData.getColumnName(i) + "\t");
    }
    System.out.println();

    while (resultSet.next()) {
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        System.out.print(resultSet.getObject(i) + "\t");
      }
      System.out.println();
    }
  }
}
