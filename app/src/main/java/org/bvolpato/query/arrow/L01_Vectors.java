package org.bvolpato.query.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

import java.nio.charset.StandardCharsets;

public class L01_Vectors {

  public static void main(String[] args) {

    System.out.println("=============================\nintVector:");

    try (BufferAllocator allocator = new RootAllocator();
        IntVector intVector = new IntVector("intVector", allocator)) {
      intVector.allocateNew(3);
      intVector.set(0, 1);
      intVector.set(1, 2);
      intVector.set(2, 3);
      intVector.setValueCount(3);
      System.out.println(intVector);
    }

    System.out.println("=============================\nvarCharVector:");

    try (BufferAllocator allocator = new RootAllocator();
        VarCharVector varCharVector = new VarCharVector("varCharVector", allocator); ) {
      varCharVector.allocateNew(3);
      varCharVector.set(0, "one".getBytes());
      varCharVector.set(1, "two".getBytes());
      varCharVector.set(2, "three".getBytes());
      varCharVector.setValueCount(3);

      System.out.println(varCharVector);
    }

    System.out.println("=============================\nDictionary:");

    try (BufferAllocator root = new RootAllocator();
        VarCharVector countries = new VarCharVector("country-dict", root);
        VarCharVector appUserCountriesUnencoded = new VarCharVector("app-use-country-dict", root)) {
      countries.allocateNew(10);
      countries.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
      countries.set(1, "Cuba".getBytes(StandardCharsets.UTF_8));
      countries.set(2, "Grecia".getBytes(StandardCharsets.UTF_8));
      countries.set(3, "Guinea".getBytes(StandardCharsets.UTF_8));
      countries.set(4, "Islandia".getBytes(StandardCharsets.UTF_8));
      countries.set(5, "Malta".getBytes(StandardCharsets.UTF_8));
      countries.set(6, "Tailandia".getBytes(StandardCharsets.UTF_8));
      countries.set(7, "Uganda".getBytes(StandardCharsets.UTF_8));
      countries.set(8, "Yemen".getBytes(StandardCharsets.UTF_8));
      countries.set(9, "Zambia".getBytes(StandardCharsets.UTF_8));
      countries.setValueCount(10);

      Dictionary countriesDictionary =
          new Dictionary(
              countries,
              new DictionaryEncoding(
                  /* id= */ 1L, /* ordered= */ false, /* indexType= */ new ArrowType.Int(8, true)));
      System.out.println(countriesDictionary);

      appUserCountriesUnencoded.allocateNew(5);
      appUserCountriesUnencoded.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
      appUserCountriesUnencoded.set(1, "Guinea".getBytes(StandardCharsets.UTF_8));
      appUserCountriesUnencoded.set(2, "Islandia".getBytes(StandardCharsets.UTF_8));
      appUserCountriesUnencoded.set(3, "Malta".getBytes(StandardCharsets.UTF_8));
      appUserCountriesUnencoded.set(4, "Uganda".getBytes(StandardCharsets.UTF_8));
      appUserCountriesUnencoded.setValueCount(5);
      System.out.println("Unencoded data: " + appUserCountriesUnencoded);

      try (FieldVector appUserCountriesDictionaryEncoded =
          (FieldVector) DictionaryEncoder.encode(appUserCountriesUnencoded, countriesDictionary)) {
        System.out.println("Dictionary-encoded data: " + appUserCountriesDictionaryEncoded);
      }
    }

    System.out.println("=============================\nlistVector:");

    try (BufferAllocator allocator = new RootAllocator();
        ListVector listVector = ListVector.empty("listVector", allocator);
        UnionListWriter listWriter = listVector.getWriter()) {
      int[] data = new int[] {1, 2, 3, 10, 20, 30, 100, 200, 300, 1000, 2000, 3000};
      int tmp_index = 0;
      for (int i = 0; i < 4; i++) {
        listWriter.setPosition(i);
        listWriter.startList();
        for (int j = 0; j < 3; j++) {
          listWriter.writeInt(data[tmp_index]);
          tmp_index = tmp_index + 1;
        }
        listWriter.setValueCount(3);
        listWriter.endList();
      }
      listVector.setValueCount(4);

      System.out.print(listVector);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
