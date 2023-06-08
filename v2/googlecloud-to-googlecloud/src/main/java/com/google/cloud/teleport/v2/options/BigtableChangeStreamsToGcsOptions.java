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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.BigtableSchemaFormat;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

public interface BigtableChangeStreamsToGcsOptions
    extends DataflowPipelineOptions, ReadChangeStreamOptions {
  @TemplateParameter.Enum(
      order = 1,
      enumOptions = {"TEXT", "AVRO"},
      optional = true,
      description = "Output file format",
      helpText =
          "The format of the output Cloud Storage file. Allowed formats are TEXT, AVRO. Default is AVRO.")
  @Default.Enum("AVRO")
  FileFormat getOutputFileFormat();

  void setOutputFileFormat(FileFormat outputFileFormat);

  @TemplateParameter.Duration(
      order = 2,
      optional = true,
      description = "Window duration",
      helpText =
          "The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for "
              + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
      example = "5m")
  @Default.String("5m")
  String getWindowDuration();

  void setWindowDuration(String windowDuration);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description = "Bigtable Metadata Table Id",
      helpText = "Table ID used for creating the metadata table."
  )
  String getBigtableMetadataTableTableId();

  void setBigtableMetadataTableTableId(String bigtableMetadataTableTableId);

  @TemplateParameter.Enum(
      order = 4,
      enumOptions = {"SIMPLE", "BIGTABLEROW"},
      optional = true,
      description = "Output schema format",
      helpText = "Schema chosen for outputting data to GCS.")
  @Default.Enum("SIMPLE")
  BigtableSchemaFormat getSchemaOutputFormat();

  void setSchemaOutputFormat(BigtableSchemaFormat outputSchemaFormat);

  @TemplateParameter.GcsWriteFolder(
      order = 5,
      description = "Output file directory in Cloud Storage",
      helpText =
          "The path and filename prefix for writing output files. Must end with a slash. "
              + "DateTime formatting is used to parse directory path for date & time formatters.",
      example = "gs://your-bucket/your-path")
  @Validation.Required
  String getGcsOutputDirectory();

  void setGcsOutputDirectory(String gcsOutputDirectory);

  @TemplateParameter.Text(
      order = 6,
      description = "Output filename prefix of the files to write",
      helpText = "The prefix to place on each windowed file.",
      example = "output-")
  @Default.String("output")
  String getOutputFilenamePrefix();

  void setOutputFilenamePrefix(String outputFilenamePrefix);

  @TemplateParameter.Integer(
      order = 7,
      optional = true,
      description = "Maximum output shards",
      helpText =
          "The maximum number of output shards produced when writing. A higher number of "
              + "shards means higher throughput for writing to Cloud Storage, but potentially higher "
              + "data aggregation cost across shards when processing output Cloud Storage files.")
  @Default.Integer(20)
  Integer getNumShards();

  void setNumShards(Integer numShards);

}
