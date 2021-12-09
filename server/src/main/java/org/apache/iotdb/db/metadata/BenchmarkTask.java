package org.apache.iotdb.db.metadata;

import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class BenchmarkTask<T> {
  private List<T> dataSet;
  private int workCount;
  private String name;
  private int timeoutInMin;

  AtomicInteger sucCounter = new AtomicInteger(0);
  AtomicInteger failCounter = new AtomicInteger(0);

  BenchmarkTask(List<T> dataSet, int workCount, String name, int timeoutInMin) {
    this.dataSet = dataSet;
    this.workCount = workCount;
    this.name = name;
    this.timeoutInMin = timeoutInMin;
  }

  List<BenchmarkResult> results = new ArrayList<>();

  public void runWork(Function<T, Boolean> work) {
    ExecutorService executor = Executors.newFixedThreadPool(workCount);
    AtomicInteger sucCounter = new AtomicInteger(0);
    AtomicInteger failCounter = new AtomicInteger(0);
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    for (T input : dataSet) {
      executor.submit(
          () -> {
            if (work.apply(input)) {
              sucCounter.incrementAndGet();
            } else {
              failCounter.incrementAndGet();
            }
          });
    }

    try {
      executor.shutdown();
      executor.awaitTermination(timeoutInMin, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    stopWatch.stop();
    BenchmarkResult result =
        new BenchmarkResult(sucCounter.get(), failCounter.get(), stopWatch.getTime());
    results.add(result);
    printReport();
  }

  private void printReport() {
    System.out.println(
        String.format("\n--------------%s------------\nsuccess    fail    cost-in-ms", name));
    for (BenchmarkResult result : results) {
      System.out.println(
          String.format("%d    %d    %d", result.successCount, result.failCount, result.costInMs));
    }
  }

  private static class BenchmarkResult {
    public long successCount;
    public long failCount;
    public long costInMs;

    BenchmarkResult(long successCount, long failCount, long cost) {
      this.successCount = successCount;
      this.failCount = failCount;
      this.costInMs = cost;
    }
  }
}
