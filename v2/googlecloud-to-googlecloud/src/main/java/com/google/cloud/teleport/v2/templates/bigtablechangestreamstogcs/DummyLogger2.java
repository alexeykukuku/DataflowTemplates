package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyLogger2 extends @UnknownKeyFor@NonNull@Initialized
    PTransform<@UnknownKeyFor @NonNull @Initialized PCollection<KV<ByteString, Iterable<ChangeStreamMutation>>>, PDone> {
  private final static Logger LOG = LoggerFactory.getLogger(DummyLogger.class);
  private final static ThreadLocal<Long> LAST_LOGGED = ThreadLocal.withInitial(() -> 0L);
  private final static AtomicLong RECORDS_PROCESSED = new AtomicLong(0);
  private final static String WORKER_ID = UUID.randomUUID().toString();
  @Override
  public PDone expand(
      @UnknownKeyFor @NonNull @Initialized PCollection<KV<ByteString, Iterable<ChangeStreamMutation>>> input) {
    input.apply("Just log periodically", ParDo.of(new DummyLogger2.LogDoFn<>()));
    return PDone.in(input.getPipeline());
  }

  private static class LogDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> r) {
      if (element instanceof Iterable) {
        for (Iterator<?> it = ((Iterable<?>) element).iterator(); ; it.hasNext()) {
          it.next();
          doOne();
        }
      } else {
        doOne();
      }
    }

    private static void doOne() {
      RECORDS_PROCESSED.incrementAndGet();

      long now = System.currentTimeMillis();
      if (LAST_LOGGED.get() < now - 60000) {
        LOG.info("Worker {} processed {} records since the last time", WORKER_ID,
            RECORDS_PROCESSED.getAndSet(0));
        LAST_LOGGED.set(now);
      }
    }
  }
}
