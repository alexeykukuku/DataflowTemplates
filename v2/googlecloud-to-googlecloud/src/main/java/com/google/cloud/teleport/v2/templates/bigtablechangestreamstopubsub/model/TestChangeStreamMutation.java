package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;
import org.threeten.bp.Instant;

public class TestChangeStreamMutation extends ChangeStreamMutation {
  private ByteString rowkey;
  private MutationType mutationType;
  private String sourceClusterId;
  private Instant commitTimestamp;
  private int tieBreaker;
  private String token;
  private Instant lowWatermark;
  private Entry entry;

  public TestChangeStreamMutation(String rowkey, MutationType mutationType,
      String sourceClusterId, Instant commitTimestamp, int tieBreaker, String token,
      Instant lowWatermark, Entry entry) {
    this.rowkey = ByteString.copyFrom(rowkey, Charset.defaultCharset());
    this.mutationType = mutationType;
    this.sourceClusterId = sourceClusterId;
    this.commitTimestamp = commitTimestamp;
    this.tieBreaker = tieBreaker;
    this.token = token;
    this.lowWatermark = lowWatermark;
    this.entry = entry;
  }
  @Nonnull
  @Override
  public ByteString getRowKey() {
    return rowkey;
  }

  @Nonnull
  @Override
  public MutationType getType() {
    return mutationType;
  }

  @Nonnull
  @Override
  public String getSourceClusterId() {
    return sourceClusterId;
  }

  @Override
  public Instant getCommitTimestamp() {
    return commitTimestamp;
  }

  @Override
  public int getTieBreaker() {
    return tieBreaker;
  }

  @Nonnull
  @Override
  public String getToken() {
    return token;
  }

  @Override
  public Instant getEstimatedLowWatermark() {
    return lowWatermark;
  }

  @Nonnull
  @Override
  public ImmutableList<Entry> getEntries() {
    return ImmutableList.<Entry>builder().add(entry).build();
  }
}
