/*
 * Copyright (C) 2022 Google LLC
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

import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToGcsOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.BigtableSchemaFormat;
import com.google.cloud.teleport.v2.utils.BigtableSource;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigtableChangeStreamsToGcs} pipeline streams change stream record(s) and stores to
 * Google Cloud Storage bucket in user specified format. The sink data can be stored in a Text or
 * Avro file format.
 */
@Template(
    name = "Bigtable_Change_Streams_to_Google_Cloud_Storage",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Bigtable change streams to Cloud Storage",
    description =
        "Streaming pipeline. Streams Bigtable change stream data records and writes them into a Cloud Storage bucket using Dataflow Runner V2.",
    flexContainerName = "bigtable-changestreams-to-gcs",
    contactInformation = "https://cloud.google.com/support",
    optionsClass = BigtableChangeStreamsToGcsOptions.class)
public class BigtableChangeStreamsToGcs {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToGcs.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  private static final String GCS_OUTPUT_DIRECTORY_REGEX = "gs://(.*?)/(.*)";

  public static void main(String[] args) {
    LOG.info("Start importing change streams data to GCS");

    BigtableChangeStreamsToGcsOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamsToGcsOptions.class);

    run(options);
  }

  private static void addCbtChangeProducer(
      Pipeline pipeline, BigtableChangeStreamsToGcsOptions options) {
    int kbPerRow = 10;
    int rate = 20000;
    int numRows = 50000000;
    String columnFamily = "cf";

    String generateLabel = String.format("Generate some data: %d rows", numRows);
    String mutationLabel = String.format("Create mutations that write %dKB to each row", kbPerRow);

    PCollection<Mutation> mutations =
        pipeline
            .apply(
                generateLabel,
                GenerateSequence.from(0).to(numRows).withRate(rate, Duration.standardSeconds(1)))
            .apply(mutationLabel, ParDo.of(new CreateMutationFn(kbPerRow * 1024L, columnFamily)));

    mutations.apply(
        String.format("Write data to table %s", options.getBigtableReadTableId()),
        CloudBigtableIO.writeToTable(
            new CloudBigtableTableConfiguration.Builder()
                .withProjectId(options.getProject())
                .withInstanceId(options.getBigtableReadInstanceId())
                .withTableId(options.getBigtableReadTableId())
                .build()));
  }

  private static String getProjectId(BigtableChangeStreamsToGcsOptions options) {
    return StringUtils.isEmpty(options.getBigtableReadProjectId())
        ? options.getProject()
        : options.getBigtableReadProjectId();
  }

  private static String getBigtableCharset(BigtableChangeStreamsToGcsOptions options) {
    return StringUtils.isEmpty(options.getBigtableChangeStreamCharset())
        ? "UTF-8"
        : options.getBigtableChangeStreamCharset();
  }

  public static PipelineResult run(BigtableChangeStreamsToGcsOptions options) {
    LOG.info("Output file format is " + options.getOutputFileFormat());
    LOG.info("Batch size is " + options.getOutputBatchSize());
    LOG.info("Maximum shards count is " + options.getOutputShardsCount());

    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    validateOutputFormat(options);
    validateOutputDirectoryAccess(options);

    final Pipeline pipeline = Pipeline.create(options);

    // Get the Bigtable project, instance, database, and change stream parameters.
    String projectId = getProjectId(options);

    // Retrieve and parse the start / end timestamps.
    Instant startTimestamp =
        options.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : toInstant(Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp()));

    BigtableSource sourceInfo =
        new BigtableSource(
            options.getBigtableReadInstanceId(),
            options.getBigtableReadTableId(),
            getBigtableCharset(options),
            options.getBigtableChangeStreamIgnoreColumnFamilies(),
            options.getBigtableChangeStreamIgnoreColumns(),
            startTimestamp);

    BigtableUtils bigtableUtils = new BigtableUtils(sourceInfo);

    // Add use_runner_v2 to the experiments option, since Change Streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    boolean hasUseRunnerV2 = false;
    for (String experiment : experiments) {
      if (experiment.equalsIgnoreCase(USE_RUNNER_V2_EXPERIMENT)) {
        hasUseRunnerV2 = true;
        break;
      }
    }
    if (!hasUseRunnerV2) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);

    Duration windowingDuration = DurationUtils.parseDuration(options.getWindowDuration());

    pipeline
        .apply(
            BigtableIO.readChangeStream()
                .withProjectId(projectId)
                .withChangeStreamName(options.getBigtableChangeStreamName())
                .withAppProfileId(options.getBigtableChangeStreamAppProfile())
                .withInstanceId(options.getBigtableReadInstanceId())
                .withTableId(options.getBigtableReadTableId())
                .withMetadataTableInstanceId(options.getBigtableChangeStreamMetadataInstanceId())
                .withMetadataTableTableId(options.getBigtableMetadataTableTableId())
                .withStartTime(startTimestamp))
        .apply(introduceTimestamps())
        .apply(
            "Creating " + options.getWindowDuration() + " Window",
            Window.<KV<ByteString, ChangeStreamMutation>>into(FixedWindows.of(windowingDuration))
                .triggering(
                    Repeatedly.forever(
                        AfterWatermark.pastEndOfWindow().withEarlyFirings(
                                AfterFirst.of(AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(windowingDuration),
                                    AfterPane.elementCountAtLeast(options.getOutputBatchSize())))
                            .withLateFirings(AfterFirst.of(
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(windowingDuration),
                                AfterPane.elementCountAtLeast(options.getOutputBatchSize())))
                    )
                ).withAllowedLateness(Duration.millis(0))
                .discardingFiredPanes())
        .apply(Values.create())
        .apply(
            "Write To GCS",
            FileFormatFactoryBigtableChangeStreams.newBuilder()
                .setOptions(options)
                .setBigtableUtils(bigtableUtils)
                .build());

    //TODO: remove this
    if (System.getenv("producer") != null) {
      addCbtChangeProducer(pipeline, options);
    }

    return pipeline.run();
  }


  private static void validateOutputFormat(BigtableChangeStreamsToGcsOptions options) {
    switch (options.getSchemaOutputFormat()) {
      case CHANGELOG_ENTRY:
        if (options.getOutputFileFormat() != FileFormat.TEXT
            && options.getOutputFileFormat() != FileFormat.AVRO) {
          throw new IllegalArgumentException(
              BigtableSchemaFormat.CHANGELOG_ENTRY
                  + " schema output format can be used with AVRO and TEXT output file formats");
        }
        break;
      case BIGTABLEROW:
        if (options.getOutputFileFormat() != FileFormat.AVRO) {
          throw new IllegalArgumentException(
              BigtableSchemaFormat.BIGTABLEROW
                  + " schema output format can be used with AVRO output file format");
        }
      default:
        throw new IllegalArgumentException(
            "Unsupported schema output format: " + options.getSchemaOutputFormat());
    }
  }

  private static WithTimestamps<KV<ByteString, ChangeStreamMutation>> introduceTimestamps() {
    return WithTimestamps.of(
        (SerializableFunction<KV<ByteString, ChangeStreamMutation>, Instant>)
            input -> {
              if (input == null || input.getValue() == null) {
                return null;
              } else {
                return Instant.ofEpochMilli(input.getValue().getCommitTimestamp().toEpochMilli());
              }
            });
  }

  private static void validateOutputDirectoryAccess(BigtableChangeStreamsToGcsOptions options)
      throws IllegalArgumentException {
    Matcher matcher =
        Pattern.compile(GCS_OUTPUT_DIRECTORY_REGEX).matcher(options.getGcsOutputDirectory());

    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "--gcsOutputDirectory is expected in a format matching "
              + "the following regular expression: "
              + GCS_OUTPUT_DIRECTORY_REGEX);
    }
    String bucket = matcher.group(1);
    String path = matcher.group(2);

    if (!path.endsWith("/")) {
      path += "/";
    }

    Storage storage =
        StorageOptions.newBuilder().setProjectId(options.getProject()).build().getService();

    String testFile = path + UUID.randomUUID();

    boolean fileCreated = false;
    try {
      BlobInfo blobInfo = BlobInfo.newBuilder(bucket, testFile).build();
      storage.create(blobInfo, new byte[0]);
      fileCreated = true;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unable to ensure write access for the output directory: "
              + options.getGcsOutputDirectory());
    } finally {
      if (fileCreated) {
        try {
          storage.delete(BlobId.of(bucket, testFile));
        } catch (Exception e) {
          LOG.warn("Unable to clean up a test file from the GCS output directory: " + path);
        }
      }
    }
  }

  // TODO: copied from CBT to BQ
  private static Instant toInstant(Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    } else {
      return Instant.ofEpochMilli(timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000);
    }
  }

  static class CreateMutationFn extends DoFn<Long, Mutation> {

    private final long rowSize;
    private final Random random = new Random();
    private final String columnFamily;

    public CreateMutationFn(long rowSize, String columnFamily) {
      this.rowSize = rowSize;
      this.columnFamily = columnFamily;
    }

    @ProcessElement
    public void processElement(@Element Long rowkey, OutputReceiver<Mutation> out) {
      long timestamp = System.currentTimeMillis();

      // Pad and reverse the rowkey for more distributed writes
      String numberFormat = "%0" + 30 + "d";
      String paddedRowkey = String.format(numberFormat, rowkey);
      String reversedRowkey = new StringBuilder(paddedRowkey).reverse().toString();
      Put row = new Put(Bytes.toBytes(reversedRowkey));

      // Generate random bytes
      byte[] randomData = new byte[(int) rowSize];

      random.nextBytes(randomData);
      row.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("C"), timestamp, randomData);
      out.output(row);
    }
  }
}
