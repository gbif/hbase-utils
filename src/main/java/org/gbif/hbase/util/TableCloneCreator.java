package org.gbif.hbase.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a new table in HBase using the splits from the existing table. Specify -Pprod or -Pdev when building to
 * point to the right cluster!
 */
public class TableCloneCreator {

  private static final Logger LOG = LoggerFactory.getLogger(TableCloneCreator.class);

  // not to be instantiated
  private TableCloneCreator() {}

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      LOG.error("Usage: org.gbif.hbase.util.TableCloneCreator <existing table> <new table to create>");
      System.exit(1);
    }

    createPresplitFromExisting(args[0], args[1]);
  }

  private static void createPresplitFromExisting(String existingTableName, String newTable) throws IOException {
    Configuration config = HBaseConfiguration.create();
    HTable existingTable = new HTable(config, Bytes.toBytes(existingTableName));
    Collection<HColumnDescriptor> existingColFams = existingTable.getTableDescriptor().getFamilies();

    HBaseAdmin admin = new HBaseAdmin(config);
    HTableDescriptor tableDescriptor = new HTableDescriptor(newTable);
    tableDescriptor.setMaxFileSize(existingTable.getTableDescriptor().getMaxFileSize());
    for (HColumnDescriptor colFam : existingColFams) {
      tableDescriptor.addFamily(colFam);
    }
    byte[][] existingStartKeys = existingTable.getStartKeys();
    // first key in getStartKeys will be empty
    byte[][] splits = Arrays.copyOfRange(existingStartKeys, 1, existingStartKeys.length);

    admin.createTable(tableDescriptor, splits);
    admin.close();
  }
}
