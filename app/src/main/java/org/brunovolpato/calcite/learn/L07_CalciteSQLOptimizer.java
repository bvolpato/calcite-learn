package org.brunovolpato.calcite.learn;

import java.sql.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.*;

public class L07_CalciteSQLOptimizer {

  public static void main(String[] args)
      throws SQLException,
          ClassNotFoundException,
          SqlParseException,
          RelConversionException,
          ValidationException {
    Class.forName("org.apache.calcite.jdbc.Driver");

    // Create a connection to Calcite using the model
    Connection connection =
        DriverManager.getConnection(
            "jdbc:calcite:model=" + L04_RelBuilder.class.getResource("/model.json").getFile());
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SqlDialect dialect = CalciteSqlDialect.DEFAULT;

    FrameworkConfig frameworkConfig =
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema.getSubSchema("schema"))
            .typeSystem(RelDataTypeSystem.DEFAULT)
            .build();
    RelBuilder builder = RelBuilder.create(frameworkConfig);

    String sql =
        """
        SELECT p."first_name", s."salary" FROM "Person" p LEFT JOIN "Salary" s ON p."position" = s."position" WHERE "age" > 30 ORDER by p."first_name"
        """;

    {
      Planner planner = Frameworks.getPlanner(frameworkConfig);

      SqlNode parsed = planner.validate(planner.parse(sql));
      RelNode relNode = planner.rel(parsed).project();
      System.out.println("Plan: " + RelOptUtil.toString(relNode));
      System.out.println(
          "Original query: "
              + new RelToSqlConverter(dialect)
                  .visitRoot(relNode)
                  .asStatement()
                  .toSqlString(dialect)
                  .getSql());

      final HepProgram hepProgram =
          new HepProgramBuilder()
              .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
              .addRuleInstance(CoreRules.PROJECT_TO_CALC)
              .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
              .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
              .addRuleInstance(CoreRules.SORT_REMOVE)
              .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE)
              .build();

      final HepPlanner hepPlanner = new HepPlanner(hepProgram);
      hepPlanner.setRoot(relNode);
      final RelNode relNodeBest = hepPlanner.findBestExp();
      final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
      final RelNode trimmed = fieldTrimmer.trim(relNodeBest);
      System.out.println("Trimmed: " + RelOptUtil.toString(trimmed));

      System.out.println();
      System.out.println(
          "Improved query: "
              + new RelToSqlConverter(dialect)
                  .visitRoot(trimmed)
                  .asStatement()
                  .toSqlString(dialect)
                  .getSql());
    }

    //    {
    //      Planner planner = Frameworks.getPlanner(frameworkConfig);
    //
    //      final VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
    //
    //      SqlNode parsedPlan = planner.parse(sql);
    //      SqlNode parsed = planner.validate(parsedPlan);
    //      RelNode relNode = planner.rel(parsed).project();
    //      System.out.println("Plan b4: " + RelOptUtil.toString(relNode));
    //
    ////      RelOptCluster cluster = RelOptCluster.create(volcanoPlanner, );
    ////      relNode = relNode.copy(relNode.getTraitSet(), List.of(relNode));
    //      volcanoPlanner.setRoot(relNode);
    //
    //      final RelNode relNodeBest = volcanoPlanner.findBestExp();
    //      final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    //      final RelNode trimmed = fieldTrimmer.trim(relNodeBest);
    //      System.out.println("Volcano: " + RelOptUtil.toString(trimmed));
    //    }

    // Close the connection when done
    connection.close();
  }
}
