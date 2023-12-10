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

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;

public class L03_QueryChanger {

  public static void main(String[] args) throws SqlParseException {

    SqlParser parser =
        SqlParser.create(
            "SELECT \"name\", SUM(\"salary\") FROM \"foo\" WHERE \"bar\"=3 GROUP BY 1");
    SqlNode query = parser.parseQuery();
    query = query.accept(new QueryModifier());

    System.out.println("===================");
    System.out.println(query);
  }

  static class QueryModifier extends SqlShuttle {
    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (id.getSimple().equals("salary")) {
        return new SqlIdentifier("annualSalary", id.getParserPosition());
      }
      return id;
    }
  }
}
