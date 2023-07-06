package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import com.google.cloud.storage.Blob;
import java.util.function.Predicate;

public abstract class CommitTimeAwarePredicate implements Predicate<Blob> {

  private Long min = null;
  private Long max = null;

  public void observeCommitTime(long commitTimeMicros) {
    if (min == null || max == null) {
      max = min = commitTimeMicros;
    } else {
      if (min > commitTimeMicros) {
        min = commitTimeMicros;
      }
      if (max < commitTimeMicros) {
        max = commitTimeMicros;
      }
    }
  }

  public Long getEarliestCommitTime() {
    return min;
  }

  public Long getLatestCommitTime() {
    return max;
  }

  public boolean found() {
    return min != null;
  }
}
