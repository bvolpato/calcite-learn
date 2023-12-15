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

package org.bvolpato.query.jelox;

import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Aggregate;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.bvolpato.query.jelox.functions.JeloxAggregationFunction;
import org.bvolpato.query.jelox.util.FunctionHelper;
import org.bvolpato.query.jelox.util.TypeHelper;
import org.jetbrains.annotations.NotNull;

public class JeloxSubstraitConsumer {
  private BufferAllocator allocator;

  public JeloxSubstraitConsumer(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public VectorSchemaRoot execute(
      io.substrait.proto.Plan protoPlan, Map<String, ArrowReader> namedTables) throws IOException {

    // Convert from proto to plan
    ProtoPlanConverter protoPlanConverter = new ProtoPlanConverter(SimpleExtension.loadDefaults());
    Plan plan = protoPlanConverter.from(protoPlan);

    Plan.Root root = plan.getRoots().get(0);
    Rel rootInput = root.getInput();
    List<Rel> inputs = rootInput.getInputs();

    ArrowReader arrowReader = null;

    for (Rel input : inputs) {
      System.out.println("input: " + input);

      if (input instanceof NamedScan namedScan) {
        List<String> tableNames = namedScan.getNames();
        System.out.println("tableNames: " + tableNames);

        // Get the ArrowReader for the table
        arrowReader = namedTables.get(tableNames.get(0));
        System.out.println("arrowReader: " + arrowReader);
      }
    }

    List<String> outputNames = root.getNames();
    System.out.println("outputNames: " + outputNames);

    // If the root is a project, then we can get the expressions
    if (rootInput instanceof Project project) {
      return processProjection(project, outputNames, arrowReader);
    } else if (rootInput instanceof Aggregate aggregate) {

      System.out.println("Aggregate: " + aggregate);

      List<Field> schemaFields = new ArrayList<>();

      for (int i = 0; i < aggregate.getMeasures().size(); i++) {
        Aggregate.Measure measure = aggregate.getMeasures().get(i);
        System.out.println("measure: " + measure);

        // Get the ArrowType based on the expression type
        Type expressionType = measure.getFunction().getType();
        FieldType arrowType = TypeHelper.getArrowFieldType(expressionType);

        Field field = new Field(outputNames.get(i), arrowType, null);
        schemaFields.add(field);
      }

      Schema arrowSchema = new Schema(schemaFields);
      VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);

      for (int i = 0; i < aggregate.getMeasures().size(); i++) {
        Aggregate.Measure measure = aggregate.getMeasures().get(i);

        JeloxAggregationFunction<?> aggregationFunction =
            FunctionHelper.getAggregationFunction(measure.getFunction());

        Object aggregated = aggregationFunction.apply(arrowReader);
        IntVector vector = (IntVector) vectorSchemaRoot.getVector(i);

        vector.setSafe(0, ((Number) aggregated).intValue());
        vector.setValueCount(1);
      }

      vectorSchemaRoot.setRowCount(1);
      return vectorSchemaRoot;
    }

    // Create ArrowSchema to return the projection

    //    ProtocolStringList name = tableName.getNamesList();
    //    ProtocolStringList tableNamesList = tableNames.getNamesList();
    //    System.out.println("tableNamesList: " + tableNamesList.toString().replace("\n", " "));

    //    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    //    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    //    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator);
    //    VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
    //    nameVector.allocateNew(3);
    //    nameVector.set(0, "David".getBytes());
    //    nameVector.set(1, "Gladis".getBytes());
    //    nameVector.set(2, "Juan".getBytes());
    //    IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
    //    ageVector.allocateNew(3);
    //    ageVector.set(0, 10);
    //    ageVector.set(1, 20);
    //    ageVector.set(2, 30);
    //    vectorSchemaRoot.setRowCount(3);

    return null;
    //        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
    //             WritableByteChannel writableByteChannel = Channels.newChannel(out);
    //             ArrowStreamWriter writer =
    //                new ArrowStreamWriter(vectorSchemaRoot, null, writableByteChannel)) {
    //          writer.start();
    //          writer.writeBatch();
    //          writer.close();
    //          System.out.println("Number of rows written: " + vectorSchemaRoot.getRowCount());
    //
    //          byte[] byteArray = out.toByteArray();
    //          return new ArrowStreamReader(
    //              new ByteArrayReadableSeekableByteChannel(byteArray), rootAllocator);
    //        }
    //      }
    //    }
  }

  @NotNull
  private VectorSchemaRoot processProjection(
      Project project, List<String> outputNames, ArrowReader arrowReader) throws IOException {
    List<Expression> expressions = project.getExpressions();
    System.out.println("expressions: " + expressions);

    List<Field> schemaFields = new ArrayList<>();
    for (int i = 0; i < expressions.size(); i++) {
      Expression expression = expressions.get(i);
      System.out.println("expression: " + expression);

      // Get the ArrowType based on the expression type
      Type expressionType = expression.getType();
      FieldType arrowType = TypeHelper.getArrowFieldType(expressionType);

      Field field = new Field(outputNames.get(i), arrowType, null);
      schemaFields.add(field);
    }

    Schema arrowSchema = new Schema(schemaFields);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);

    // System.out.println(arrowReader.getVectorSchemaRoot().contentToTSVString());

    while (arrowReader.loadNextBatch()) {
      VectorSchemaRoot sourceTable = arrowReader.getVectorSchemaRoot();

      for (String outputName : outputNames) {
        FieldVector vec = vectorSchemaRoot.getVector(outputName);
        FieldVector sourceVector = sourceTable.getVector(outputName);
        TransferPair transferPair = sourceVector.makeTransferPair(vec);
        transferPair.splitAndTransfer(0, sourceVector.getValueCount());
      }

      vectorSchemaRoot.setRowCount(sourceTable.getRowCount());
    }

    return vectorSchemaRoot;
  }
}
