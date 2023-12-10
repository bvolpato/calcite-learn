package org.brunovolpato.calcite.learn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class L08_Linq4j {
  public static void main(String[] args) throws ClassNotFoundException, SQLException {

    Class.forName("org.apache.calcite.jdbc.Driver");

    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    List<Employee> employees = new ArrayList<>();
    employees.add(new Employee(1L, "Bruno", 31, "Software Engineer"));
    employees.add(new Employee(2L, "John", 37, "Engineering Manager"));
    employees.add(new Employee(3L, "Smith", 65, "CEO"));
    employees.add(new Employee(4L, "Jennifer", 45, "CFO"));

    List<Salary> salaries = new ArrayList<>();
    salaries.add(new Salary("Software Engineer", 100000.0));
    salaries.add(new Salary("Engineering Manager", 120000.0));
    salaries.add(new Salary("CEO", 150000.0));

    rootSchema.add("employee", new EmployeeTable(employees));
    rootSchema.add("salary", new SalaryTable(salaries));

    Statement statement = calciteConnection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "select e.*, s.* from `employee` as e LEFT OUTER JOIN `salary` as s ON (s.`position` = e.`position`) where e.`age` > 0 and e.`name` LIKE '%'");

    while (rs.next()) {

      long id = rs.getLong("id");
      String name = rs.getString("name");
      int age = rs.getInt("age");
      String position = rs.getString("position");
      double salary = rs.getDouble("salary");
      System.out.println(
          "id: "
              + id
              + "; name: "
              + name
              + "; age: "
              + age
              + "; position: "
              + position
              + "; salary: "
              + salary);
    }

    rs.close();
    statement.close();
    connection.close();
  }
}

class Employee {
  long id;
  String name;
  int age;
  String position;

  public Employee(long id, String name, int age, String position) {
    this.id = id;
    this.name = name;
    this.age = age;
    this.position = position;
  }
}

class Salary {
  String position;
  double salary;

  public Salary(String position, double salary) {
    this.position = position;
    this.salary = salary;
  }
}

abstract class CalciteTable<T> extends AbstractTable implements ScannableTable {

  protected List<T> data;

  public CalciteTable(List<T> data) {
    this.data = data;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> types =
        getFieldTypes().stream().map(typeFactory::createSqlType).collect(Collectors.toList());
    return typeFactory.createStructType(types, getFieldNames());
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return Linq4j.asEnumerable(data.stream().map(getMapFunction()).collect(Collectors.toList()));
  }

  protected abstract List<String> getFieldNames();

  protected abstract List<SqlTypeName> getFieldTypes();

  protected abstract Function<T, Object[]> getMapFunction();
}

class EmployeeTable extends CalciteTable<Employee> {

  public EmployeeTable(List<Employee> data) {
    super(data);
  }

  @Override
  protected List<String> getFieldNames() {
    return List.of("id", "name", "age", "position");
  }

  @Override
  protected List<SqlTypeName> getFieldTypes() {
    return List.of(
        SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.VARCHAR);
  }

  @Override
  protected Function<Employee, Object[]> getMapFunction() {
    return employee -> new Object[] {employee.id, employee.name, employee.age, employee.position};
  }
}

class SalaryTable extends CalciteTable<Salary> {

  public SalaryTable(List<Salary> data) {
    super(data);
  }

  @Override
  protected List<String> getFieldNames() {
    return List.of("position", "salary");
  }

  @Override
  protected List<SqlTypeName> getFieldTypes() {
    return List.of(SqlTypeName.VARCHAR, SqlTypeName.DOUBLE);
  }

  @Override
  protected Function<Salary, Object[]> getMapFunction() {
    return salary -> new Object[] {salary.position, salary.salary};
  }
}
