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

package org.bvolpato.query.calcite;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class L01_QueryParser {

  public static void main(String[] args) throws SqlParseException {

    explain("SELECT * FROM \"foo\" WHERE \"bar\"=3 GROUP BY \"baz\"");
    explain("SELECT name, SUM(salary) FROM foo WHERE bar=3 GROUP BY 1");
    explain(
        "UPDATE Person SET FirstName = 'NewFirstName', LastName = 'NewLastName' WHERE PersonID = 123");
  }

  public static SqlNode explain(String queryString) throws SqlParseException {
    SqlParser parser = SqlParser.create(queryString);
    SqlNode query = parser.parseQuery();

    System.out.println("=====================");
    System.out.println("Original: " + queryString);
    System.out.println("Parsed: ");
    System.out.println(query);
    System.out.println();
    System.out.println("Type: " + query.getClass());
    System.out.println("Kind: " + query.getKind());

    if (query instanceof SqlSelect select) {
      System.out.println("Select list: " + select.getSelectList());
      System.out.println("Where: " + select.getWhere());
      System.out.println("Group by: " + select.getGroup());
      System.out.println("From: " + select.getFrom());
    }
    return query;
  }
}
