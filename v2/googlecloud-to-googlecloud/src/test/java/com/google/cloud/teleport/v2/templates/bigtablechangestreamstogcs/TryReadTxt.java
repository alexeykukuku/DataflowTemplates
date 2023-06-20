/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;

public class TryReadTxt {

  private static final Gson gson = new Gson();

  public static void main(String[] args) throws Exception {
    File file =
        new File(
            "/usr/local/google/home/alexeyku/Dev/CDCGCS/v2/googlecloud-to-googlecloud/whacky.txt");
    byte[] fileBytes = IOUtils.readFully(new FileInputStream(file), (int) file.length());

    SpecificDatumReader<ChangelogEntry> reader = new SpecificDatumReader<>(ChangelogEntry.class);

    // Schema schema = new Schema.Parser().parse(new
    // File("/usr/local/google/home/alexeyku/Dev/CDCGCS/v2/googlecloud-to-googlecloud/src/main/resources/schema/avro/changelogentry.avsc"));
    // JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, new
    // ByteArrayInputStream(fileBytes));
    //
    // ChangelogEntry entry = reader.read(null, jsonDecoder);

    ChangelogEntry entry = gson.fromJson(new FileReader(file), ChangelogEntry.class);

    System.out.println(entry);

    //   ChangelogEntry value = null;
    //   do {
    //     value = reader.read(null, josnDecoder);
    //     System.out.println(value);
    //   } while(value != null);
    //
    // }
  }
}
