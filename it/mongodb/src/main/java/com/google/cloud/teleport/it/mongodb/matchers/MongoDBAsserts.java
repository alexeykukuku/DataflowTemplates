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
package com.google.cloud.teleport.it.mongodb.matchers;

import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatRecords;

import com.google.cloud.teleport.it.truthmatchers.RecordsSubject;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.bson.Document;

/** Assert utilities for Splunk tests. */
public class MongoDBAsserts {

  /**
   * Convert MongoDB {@link org.bson.Document} to a list of maps.
   *
   * @param documents List of Documents to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> mongoDBDocumentsToRecords(Iterable<Document> documents) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Document document : documents) {
        Map<String, Object> converted =
            new Gson().<Map<String, Object>>fromJson(document.toJson(), Map.class);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting MongoDB Documents to Records", e);
    }
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param documents List of Documents in MongoDB {@link Document} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatMongoDBDocuments(
      @Nullable Collection<Document> documents) {
    return assertThatRecords(mongoDBDocumentsToRecords(documents));
  }
}
