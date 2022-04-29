package com.salesforce.hbase;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarksMain {

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(BenchmarkAircompressorLz4.class.getSimpleName())
      .include(BenchmarkAircompressorLzo.class.getSimpleName())
      .include(BenchmarkAircompressorSnappy.class.getSimpleName())
      .include(BenchmarkAircompressorZstd.class.getSimpleName())
      .include(BenchmarkBrotli.class.getSimpleName())
      .include(BenchmarkLz4.class.getSimpleName())
      .include(BenchmarkLzma.class.getSimpleName())
      .include(BenchmarkSnappy.class.getSimpleName())
      .include(BenchmarkZstd.class.getSimpleName())
      .forks(1)
      .build();
    new Runner(opt).run();
  }

}
