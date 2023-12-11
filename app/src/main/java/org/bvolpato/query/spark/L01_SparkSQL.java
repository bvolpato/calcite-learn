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

package org.bvolpato.query.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// https://spark.apache.org/docs/latest/sql-getting-started.html
public class L01_SparkSQL {

  private static final String BASE_DIR = System.getProperty("user.home");

  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder().appName("Java Spark").master("local[*]").getOrCreate();

    Dataset<Row> nations =
        spark.read().parquet("file://" + BASE_DIR + "/Downloads/tpch/nation.parquet");
    nations.show();

    Dataset<Row> customers =
        spark.read().parquet("file://" + BASE_DIR + "/Downloads/tpch/customer.parquet");
    customers.show(10);

    nations.createOrReplaceTempView("nation");
    customers.createOrReplaceTempView("customer");

    Dataset<Row> result =
        spark.sql(
            """
            SELECT n.n_name, ROUND(AVG(C_ACCTBAL), 2) AS BALANCES FROM NATION n JOIN CUSTOMER c
                            ON n.n_nationkey = c.c_nationkey
                            GROUP BY n.n_name
                            ORDER BY BALANCES DESC LIMIT 10
            """);
    result.show();
  }
}
