package org.gbif.hbase.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSplitCounter {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseSplitCounter.class);

  private static void generateSplitsFile(String tableName, String outFileName) throws IOException {
    LOG.info("Finding splits for [{}]", tableName);

    FileOutputStream fos = null;
    OutputStreamWriter writer = null;
    try {
      File outFile = new File(outFileName);
      outFile.delete();
      outFile.createNewFile();
      fos = new FileOutputStream(outFile);
      writer = new OutputStreamWriter(fos, "UTF-8");

      Configuration config = HBaseConfiguration.create();
      NavigableMap<HRegionInfo, ServerName> regions =
        MetaScanner.allTableRegions(config, Bytes.toBytes(tableName), false);
      int goodSplits = 0;
      for (HRegionInfo region : regions.keySet()) {
        byte[] endKey = region.getEndKey();
        if (endKey.length > 0) {
          int goodKey = Bytes.toInt(endKey);
          goodSplits++;
          writer.write(goodKey + "\n");
          writer.flush();
          LOG.info("Got endkey [{}]", goodKey);
        }
      }

      LOG.info("Finished getting [{}] endkey splits for [{}] regions in [{}]", goodSplits, regions.size(), tableName);
    } catch (FileNotFoundException e) {
      LOG.error("Couldn't create file [{}] - aborting", outFileName, e);
      System.exit(1);
    } catch (IOException e) {
      LOG.warn("IOException when writing to file", e);
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
        if (fos != null) {
          fos.close();
        }
      } catch (IOException e) {
        LOG.warn("Couldn't close file connections", e);
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      LOG.error("Usage: HBaseSplitCounter <tableName> <outputFileName>");
      System.exit(1);
    }
    generateSplitsFile(args[0], args[1]);
  }
}
