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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSplitCounter {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseSplitCounter.class);

  private static File reCreate(String fileName) throws IOException {
    File file = new File(fileName);
    Files.deleteIfExists(file.toPath());
    Files.createFile(file.toPath());
    return file;
  }

  private static void generateSplitsFile(String tableName, String outFileName) throws IOException {
    LOG.info("Finding splits for [{}]", tableName);

    File outFile = reCreate(outFileName);

    try (FileOutputStream fos = new FileOutputStream(outFile);
         OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
      Configuration config = HBaseConfiguration.create();
      TableName hTableName = TableName.valueOf(tableName);
      try(Connection connection = ConnectionFactory.createConnection(config);
          RegionLocator regionLocator = connection.getRegionLocator(hTableName)) {
        List<HRegionLocation> regions = regionLocator.getAllRegionLocations();
        int goodSplits = 0;
        for (HRegionLocation region : regions) {
          byte[] endKey = region.getRegion().getEndKey();
          if (endKey.length > 0) {
            int goodKey = Bytes.toInt(endKey);
            goodSplits++;
            writer.write(goodKey + "\n");
            writer.flush();
            LOG.info("Got endkey [{}]", goodKey);
          }
        }
        LOG.info("Finished getting [{}] endkey splits for [{}] regions in [{}]", goodSplits, regions.size(), tableName);
      }
    } catch (FileNotFoundException e) {
      LOG.error("Couldn't create file [{}] - aborting", outFileName, e);
      System.exit(1);
    } catch (IOException e) {
      LOG.warn("IOException when writing to file", e);
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
