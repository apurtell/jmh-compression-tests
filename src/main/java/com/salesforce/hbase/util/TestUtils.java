package com.salesforce.hbase.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

public class TestUtils {

  public static final String BLOCK_DATA_RESOURCE = "blockdata.zip";

  public static List<byte[]> expandZipResource(String resource) throws IOException {
    List<byte[]> results = new ArrayList<>();
    byte[] buffer = new byte[8192];
    InputStream resourceIn = TestUtils.class.getClassLoader().getResourceAsStream(resource);
    if (resourceIn == null) {
      throw new NullPointerException("getResourceAsStream returned null for '" + resource + "'");
    }
    try (ZipInputStream in = new ZipInputStream(resourceIn, StandardCharsets.UTF_8)) {
      ZipEntry e = in.getNextEntry();
      while (e != null) {
        try {
          ByteArrayOutputStream os = new ByteArrayOutputStream();
          int n;
          while ((n = in.read(buffer, 0, buffer.length)) > 0) {
            os.write(buffer, 0, n);
          }
          results.add(os.toByteArray());
        } finally {
          in.closeEntry();
        }
        e = in.getNextEntry();
      }
    }
    return results;
  }

  public static long outputStreamTest(CompressionCodec codec, List<byte[]> blockData)
      throws IOException {
    return outputStreamTest(codec, codec.createCompressor(), blockData);
  }

  public static long outputStreamTest(CompressionCodec codec, Compressor compressor, 
      List<byte[]> blockData) throws IOException {
    long totalSize = 0;
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] block: blockData) {
      try (CompressionOutputStream os = codec.createOutputStream(out, compressor)) {
        os.write(block);
      }
      totalSize += out.size();
      out.reset();
      compressor.reset();
    }
    return totalSize;
  }

}
