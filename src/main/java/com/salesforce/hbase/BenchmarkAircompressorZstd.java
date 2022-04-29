package com.salesforce.hbase;

import com.salesforce.hbase.util.TestUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.aircompressor.ZstdCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class BenchmarkAircompressorZstd {

  List<byte[]> blockData;
  long blockDataTotalSize;
  long compressedDataTotalSize;
  CompressionCodec codec;

  @Setup
  public void setup() throws Exception {
    final String className = ZstdCodec.class.getName();
    final Configuration conf = new Configuration();
    conf.set(Compression.ZSTD_CODEC_CLASS_KEY, className);
    Compression.Algorithm.ZSTD.reload(conf);
    codec = new ZstdCodec();
    blockData = TestUtils.expandZipResource(TestUtils.BLOCK_DATA_RESOURCE);
    blockDataTotalSize = 0;
    for (byte[] block: blockData) {
      blockDataTotalSize += block.length;
    }
    System.out.println("Prepared " + blockData.size() + " blocks (" + blockDataTotalSize +
      " bytes total) from " + TestUtils.BLOCK_DATA_RESOURCE);
  }

  @TearDown
  public void tearDown() {
    System.out.println("Compressed " + blockDataTotalSize + " -> " + compressedDataTotalSize +
      " bytes, " + String.format("(%02f%%)",
         100.0 - (100.0f * ((float)compressedDataTotalSize / (float)blockDataTotalSize))));    
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void test() throws Exception {
    compressedDataTotalSize = TestUtils.outputStreamTest(codec, blockData);
  }

}
