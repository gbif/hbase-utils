/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.hbase.util;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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

    createPreSplitFromExisting(args[0], args[1]);
  }

  private static void createPreSplitFromExisting(String existingTableName, String newTable) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
         Table existingTable = connection.getTable(TableName.valueOf(existingTableName));
         Admin admin = connection.getAdmin()) {

      TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(newTable))
                                                .setMaxFileSize(existingTable.getDescriptor().getMaxFileSize());
      tableDescriptor.setColumnFamilies(Arrays.asList(existingTable.getDescriptor().getColumnFamilies()));

      byte[][] existingStartKeys = existingTable.getRegionLocator().getStartKeys();
      // first key in getStartKeys will be empty
      byte[][] splits = Arrays.copyOfRange(existingStartKeys, 1, existingStartKeys.length);

      admin.createTable(tableDescriptor.build(), splits);
    }
  }
}
