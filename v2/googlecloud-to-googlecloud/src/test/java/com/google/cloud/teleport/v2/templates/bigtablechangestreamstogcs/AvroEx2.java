package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.common.collect.Iterators;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;

public class AvroEx2 {

  public static void main(String... args) throws Exception {
    File outputFile = new File("/usr/local/google/home/alexeyku/Dev/CDCGCS/specimen.avro");
    List<ChangelogEntry> actualElements = new ArrayList<>();
    try (DataFileReader<ChangelogEntry> reader =
        new DataFileReader<>(
            outputFile,
            new ReflectDatumReader<>(ReflectData.get().getSchema(ChangelogEntry.class)))) {

      Iterators.addAll(actualElements, reader);
    }
    System.out.println(actualElements);
  }
}
