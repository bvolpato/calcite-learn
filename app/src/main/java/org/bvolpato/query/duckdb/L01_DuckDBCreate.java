package org.bvolpato.query.duckdb;

import java.sql.*;

public class L01_DuckDBCreate {
  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.duckdb.DuckDBDriver");
    // Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    Connection conn = DriverManager.getConnection("jdbc:duckdb:/tmp/sampleduck.db");

    // create a table
    Statement stmt = conn.createStatement();
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)");

    // insert two items into the table
    stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");

    ResultSet rs = stmt.executeQuery("SELECT * from items");
    while (rs.next()) {
      System.out.println("ResultSet: " + rs.getString("item") + ", " + rs.getInt("count"));
    }

    stmt.execute(
        "CREATE TABLE IF NOT EXISTS nation  AS SELECT * FROM read_parquet('/home/bvolpato/Downloads/tpch/nation.parquet')");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS customer  AS SELECT * FROM read_parquet('/home/bvolpato/Downloads/tpch/customer.parquet')");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS lineitem  AS SELECT * FROM read_parquet('/home/bvolpato/Downloads/tpch/lineitem.parquet')");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS orders  AS SELECT * FROM read_parquet('/home/bvolpato/Downloads/tpch/orders.parquet')");
  }
}
