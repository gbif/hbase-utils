package org.gbif.hbase.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSplitReader {

  private static final Logger LOG = LoggerFactory.getLogger(TableCloneCreator.class);

  // not to be instantiated
  private TableSplitReader() {
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      LOG.error("Usage: org.gbif.hbase.util.TableSplitReader <existing table>");
      System.exit(1);
    }

    readIntSplits(args[0]);
  }

  private static void readIntSplits(String existingTableName) throws IOException {
    Configuration config = HBaseConfiguration.create();
    HTable existingTable = new HTable(config, Bytes.toBytes(existingTableName));

    String fileName = existingTableName + "_splits.txt";
    LOG.info("Writing file [{}]", fileName);
    File outFile = new File(fileName);
    FileWriter fileWriter = new FileWriter(outFile);
    byte[][] existingStartKeys = existingTable.getStartKeys();
    for (int i = 1; i < existingStartKeys.length; i++) {
      fileWriter.write(Bytes.toInt(existingStartKeys[i]) + "\n");
    }

    fileWriter.close();
  }
}
