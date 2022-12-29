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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableCreator {

  private static Logger log = LoggerFactory.getLogger(org.gbif.hbase.util.TableCreator.class);

  /**
   * Create an hbase table with given colfamily, using the given splits file to presplit the table.  Regions are set
   * to the given max regionSize (given in MB and translated into bytes). Input table
   *
   * @param tableName      table to create
   * @param colFamily      col family to create
   * @param splitsFileName one split per line, interpreted as ints
   * @param regionSize     max region file size in MB
   */
  private void createPreSplitTable(String tableName, String colFamily, String splitsFileName, Integer regionSize) {
    byte[][] byteSplits = null;

    if (splitsFileName != null) {
      // assumes splits are written as strings but are to be interpreted as ints
      log.info("Reading splits file [{}]", splitsFileName);
      List<Integer> splits = new ArrayList<Integer>();
      try {
        BufferedReader reader =
          new BufferedReader(new InputStreamReader(new FileInputStream(new File(splitsFileName)), "UTF-8"));
        while (reader.ready()) {
          String line = reader.readLine();
          splits.add(Integer.valueOf(line));
        }
      } catch (FileNotFoundException e) {
        log.warn("Splits file not found - exiting", e);
        System.exit(1);
      } catch (UnsupportedEncodingException e) {
        log.error("Drastic jvm failure - exiting", e);
        System.exit(1);
      } catch (IOException e) {
        log.warn("Failure reading splits file - exiting", e);
        System.exit(1);
      }
      log.info("Loaded [{}] splits", splits.size());

      byteSplits = new byte[splits.size()][];
      for (int i = 0; i < splits.size(); i++) {
        byteSplits[i] = Bytes.toBytes(splits.get(i));
      }
    }

    log.info("Creating table");
    try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
         Admin admin = connection.getAdmin()) {

      ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder
        .newBuilder(colFamily.getBytes(StandardCharsets.UTF_8))
        .setCompressionType(Compression.Algorithm.SNAPPY)
        .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
        .setMaxVersions(1)
        .build();
      TableDescriptorBuilder ror = TableDescriptorBuilder
        .newBuilder(TableName.valueOf(tableName))
        .setColumnFamily(cf);

      if (regionSize != null && regionSize > 0) {
        ror.setMaxFileSize(regionSize * 1048576); // 1 MB = 2^20 bytes
      }

      if (byteSplits == null) {
        admin.createTable(ror.build());
      } else {
        admin.createTable(ror.build(), byteSplits);
      }
    } catch (IOException e) {
      log.info("HBase exception", e);
      System.exit(1);
    }
    log.info("Table created - exiting");
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Usage: TableCreator <table_name> <column_family> [<splits_file>] [<region_file_size_mb>]");
      System.exit(1);
    }
    String tableName = args[0];
    String colFam = args[1];
    String splitsFile = args.length > 2 ? args[2] : null;
    Integer regionSize = args.length == 4 ? Integer.valueOf(args[3]) : null;

    TableCreator instance = new TableCreator();
    //    instance.createPreSplitTable("ror_120", "v", "/Users/oliver/SourceCode/ror_120_splits.txt", 256*3);
    instance.createPreSplitTable(tableName, colFam, splitsFile, regionSize);
  }
}
