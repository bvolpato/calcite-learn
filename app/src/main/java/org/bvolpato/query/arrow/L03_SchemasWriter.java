package org.bvolpato.query.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class L03_SchemasWriter {

  public static void main(String[] args) {

    Schema schemaPerson = getPersonSchema();
    System.out.println(schemaPerson);

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schemaPerson, allocator)) {
      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      nameVector.allocateNew(3);
      nameVector.set(0, "David".getBytes());
      nameVector.set(1, "Gladis".getBytes());
      nameVector.set(2, "Juan".getBytes());
      nameVector.setValueCount(3);
      IntVector ageVector = (IntVector) root.getVector("age");
      ageVector.allocateNew(3);
      ageVector.set(0, 10);
      ageVector.set(1, 20);
      ageVector.set(2, 30);
      ageVector.setValueCount(3);

      ListVector listVector = (ListVector) root.getVector("points");

      UnionListWriter listWriter = listVector.getWriter();

      for (int i = 0; i < 3; i++) {
        listWriter.setPosition(i);
        listWriter.startList();
        listWriter.writeInt(4 + i);
        listWriter.writeInt(8 + i);
        listWriter.writeInt(16 + i);
        listWriter.endList();
      }

      listVector.setValueCount(3);
      root.setRowCount(3);

      System.out.print(root.contentToTSVString());

      File file = new File("/tmp/output_l03.arrow");
      try (FileOutputStream fileOutputStream = new FileOutputStream(file);
          ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
        writer.start();
        writer.writeBatch();
        writer.end();
        System.out.println(
            "Record batches written: "
                + writer.getRecordBlocks().size()
                + ". Number of rows written: "
                + root.getRowCount());
      } catch (IOException e) {
        e.printStackTrace();
      }

      File streamFile = new File("/tmp/output_l03_stream.arrow");
      try (FileOutputStream fileOutputStream = new FileOutputStream(streamFile);
          ArrowStreamWriter writer =
              new ArrowStreamWriter(root, null, fileOutputStream.getChannel())) {
        writer.start();
        writer.writeBatch();
        System.out.println("Number of rows written: " + root.getRowCount());
      } catch (IOException e) {
        e.printStackTrace();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static Schema getPersonSchema() {
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /* dictionary= */ null);
    FieldType listType = new FieldType(true, new ArrowType.List(), /* dictionary= */ null);
    Field childField = new Field("intCol", intType, null);
    List<Field> childFields = new ArrayList<>();
    childFields.add(childField);
    Field points = new Field("points", listType, childFields);
    Schema schemaPerson = new Schema(asList(name, age, points));
    return schemaPerson;
  }
}
