package com.salesforce.hbase;

import com.salesforce.hbase.util.TestUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.brotli.BrotliCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class BenchmarkBrotli {

  @Param("6")
  int level;
  @Param("24")
  int window;

  Configuration conf;
  CompressionCodec codec;
  List<byte[]> blockData;
  long blockDataTotalSize;
  long compressedDataTotalSize;

  @Setup
  public void setup() throws Exception {
    final String className = BrotliCodec.class.getName();
    conf = new Configuration();
    conf.set(Compression.BROTLI_CODEC_CLASS_KEY, className);
    Compression.Algorithm.BROTLI.reload(conf);
    codec = new BrotliCodec();
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
    Compressor compressor = codec.createCompressor();
    conf.setInt(BrotliCodec.BROTLI_LEVEL_KEY, level);
    conf.setInt(BrotliCodec.BROTLI_WINDOW_KEY, window);
    compressor.reinit(conf);
    compressedDataTotalSize = TestUtils.outputStreamTest(codec, compressor, blockData);
  }

}
