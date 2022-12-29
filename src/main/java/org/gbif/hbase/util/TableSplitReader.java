package org.gbif.hbase.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
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

  private static File splitsFile(String tableName) {
    String fileName = tableName + "_splits.txt";
    return new File(fileName);
  }
  private static void readIntSplits(String existingTableName) throws IOException {
    Configuration config = HBaseConfiguration.create();
    File outFile = splitsFile(existingTableName);
    try (FileWriter fileWriter = new FileWriter(outFile);
         Connection connection = ConnectionFactory.createConnection(config);
         Table existingTable = connection.getTable(TableName.valueOf(existingTableName))) {
      LOG.info("Writing file [{}]", outFile.getName());
      byte[][] existingStartKeys = existingTable.getRegionLocator().getStartKeys();
      for (int i = 1; i < existingStartKeys.length; i++) {
        fileWriter.write(Bytes.toInt(existingStartKeys[i]) + "\n");
      }
  }
  }
}
