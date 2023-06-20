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

import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.bigtable.ChangelogEntryJson;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.function.Predicate;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookForChangelogEntryJsonRecord implements Predicate<Blob> {
  private final RowMutation recordToLookFor;

  public LookForChangelogEntryJsonRecord(RowMutation rowMutation) {
    this.recordToLookFor = rowMutation;
  }

  @Override
  public boolean test(Blob o) {
    byte[] content = o.getContent();
    ChangelogEntryJson changelogEntry = (new Gson()).fromJson(new String(content), ChangelogEntryJson.class);


    return false;
  }
}
