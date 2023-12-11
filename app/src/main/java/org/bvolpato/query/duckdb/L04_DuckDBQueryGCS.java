package org.bvolpato.query.duckdb;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import org.brunocvcunha.inutils4j.MyStringUtils;

public class L04_DuckDBQueryGCS {

  private static final String BASE_DIR = System.getProperty("user.home");

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    Class.forName("org.duckdb.DuckDBDriver");

    Connection conn = DriverManager.getConnection("jdbc:duckdb:/tmp/sampleduck.db");

    // create a table
    Statement stmt = conn.createStatement();
    stmt.execute(MyStringUtils.getContent(new File(BASE_DIR + "/Downloads/duckdb_creds.txt")));

    ResultSet rs =
        stmt.executeQuery("SELECT * from read_parquet('s3://<BUCKET>/tpch/nation.parquet')");
    ResultSetMetaData metaData = rs.getMetaData();

    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      System.out.print(metaData.getColumnName(i) + "\t");
    }
    System.out.println();

    while (rs.next()) {
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        System.out.print(rs.getObject(i) + "\t");
      }
      System.out.println();
    }
  }
}
