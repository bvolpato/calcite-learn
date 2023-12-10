package org.brunovolpato.calcite.learn;

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
