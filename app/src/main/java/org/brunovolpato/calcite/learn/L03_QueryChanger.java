package org.brunovolpato.calcite.learn;

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
