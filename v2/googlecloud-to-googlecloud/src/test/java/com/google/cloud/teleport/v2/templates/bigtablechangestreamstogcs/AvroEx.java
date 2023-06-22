package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import com.google.cloud.teleport.bigtable.ChangelogEntry;
import java.io.File;
import java.io.FileInputStream;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroEx {

  public static void main(String... args) throws Exception {
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("/usr/local/google/home/alexeyku/Dev/CDCGCS/specimen.avro"), reader);
    while (dataFileReader.hasNext()) {
      GenericRecord grec = dataFileReader.next();
      System.out.println(grec.toString());
    }
  }
}
