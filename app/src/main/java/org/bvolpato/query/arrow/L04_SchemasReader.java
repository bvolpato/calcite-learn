package org.bvolpato.query.arrow;

import static java.util.Arrays.asList;
import static org.bvolpato.query.arrow.L03_SchemasWriter.getPersonSchema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class L04_SchemasReader {

  public static void main(String[] args) {

    File file = new File("/tmp/output_l03.arrow");
    try (BufferAllocator rootAllocator = new RootAllocator();
        FileInputStream fileInputStream = new FileInputStream(file);
        ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)) {
      System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
        System.out.println(vectorSchemaRootRecover.contentToTSVString());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("Streaming...");

    File streamFile = new File("/tmp/output_l03_stream.arrow");

    try (BufferAllocator rootAllocator = new RootAllocator();
        FileInputStream fileInputStream = new FileInputStream(streamFile);
        ArrowStreamReader reader =
            new ArrowStreamReader(fileInputStream.getChannel(), rootAllocator)) {

      while (reader.loadNextBatch()) {
        VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
        System.out.println(vectorSchemaRootRecover.contentToTSVString());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
