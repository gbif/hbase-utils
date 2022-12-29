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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ResultReaderTest {

  private static final String CF1_NAME = "1";
  private static final byte[] CF1 = Bytes.toBytes(CF1_NAME);
  private static final String CF2_NAME = "2";
  private static final byte[] CF2 = Bytes.toBytes(CF2_NAME);
  private static final String INT_COL_NAME = "a";
  private static final byte[] INT_COL = Bytes.toBytes(INT_COL_NAME);
  private static final String DOUBLE_COL_NAME = "b";
  private static final byte[] DOUBLE_COL = Bytes.toBytes(DOUBLE_COL_NAME);
  private static final String LONG_COL_NAME = "c";
  private static final byte[] LONG_COL = Bytes.toBytes(LONG_COL_NAME);
  private static final String STRING_COL_NAME = "d";
  private static final byte[] STRING_COL = Bytes.toBytes(STRING_COL_NAME);
  private static final String BYTES_COL_NAME = "e";
  private static final byte[] BYTES_COL = Bytes.toBytes(BYTES_COL_NAME);
  private static final byte[] KEY = Bytes.toBytes("12345");

  private static final int INT_VAL_1 = 1111;
  private static final double DOUBLE_VAL_1 = 2.2222222222222222222d;
  private static final long LONG_VAL_1 = 33333333333333l;
  private static final String STRING_VAL_1 = "not numbers";
  private static final byte[] BYTES_VAL_1 = Bytes.toBytes("byte values");

  private static final int INT_VAL_2 = 4444;
  private static final double DOUBLE_VAL_2 = 5.55555555555555d;
  private static final long LONG_VAL_2 = 66666666666666666l;
  private static final String STRING_VAL_2 = "just a string";
  private static final byte[] BYTES_VAL_2 = Bytes.toBytes("different byte values");

  private Result result = null;

  private static Cell newCell(byte[] row, byte[] columnFamily, byte[] qualifier, byte[] value) {
    return CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
            .setRow(row)
            .setFamily(columnFamily)
            .setQualifier(qualifier)
            .setValue(value)
            .setType(Cell.Type.Put)
            .build();
  }
  @Before
  public void setup() {
    List<Cell> cells = new ArrayList<>();

    // CF1
    Cell cell = newCell(KEY, CF1, INT_COL, Bytes.toBytes(INT_VAL_1));
    cells.add(cell);
    cell = newCell(KEY, CF1, DOUBLE_COL, Bytes.toBytes(DOUBLE_VAL_1));
    cells.add(cell);
    cell = newCell(KEY, CF1, LONG_COL, Bytes.toBytes(LONG_VAL_1));
    cells.add(cell);
    cell = newCell(KEY, CF1, STRING_COL, Bytes.toBytes(STRING_VAL_1));
    cells.add(cell);
    cell = newCell(KEY, CF1, BYTES_COL, BYTES_VAL_1);
    cells.add(cell);

    // CF2
    cell = newCell(KEY, CF2, INT_COL, Bytes.toBytes(INT_VAL_2));
    cells.add(cell);
    cell = newCell(KEY, CF2, DOUBLE_COL, Bytes.toBytes(DOUBLE_VAL_2));
    cells.add(cell);
    cell = newCell(KEY, CF2, LONG_COL, Bytes.toBytes(LONG_VAL_2));
    cells.add(cell);
    cell = newCell(KEY, CF2, STRING_COL, Bytes.toBytes(STRING_VAL_2));
    cells.add(cell);
    cell = newCell(KEY, CF2, BYTES_COL, BYTES_VAL_2);
    cells.add(cell);
    result = Result.create(cells);
  }

  @Test
  public void testString() {

    String test = ResultReader.getString(result, CF1_NAME, STRING_COL_NAME, null);
    assertTrue(STRING_VAL_1.equals(test));

    test = ResultReader.getString(result, CF1_NAME, "fake col", "a default value");
    assertTrue("a default value".equals(test));

    test = ResultReader.getString(result, CF2_NAME, STRING_COL_NAME, null);
    assertTrue(STRING_VAL_2.equals(test));
  }

  @Test
  public void testDouble() {
    Double test = ResultReader.getDouble(result, CF1_NAME, DOUBLE_COL_NAME, null);
    assertEquals(DOUBLE_VAL_1, test, 0.00001);

    test = ResultReader.getDouble(result, CF1_NAME, "fake col", 123456.789d);
    assertEquals(123456.789, test, 0.00001);

    test = ResultReader.getDouble(result, CF2_NAME, DOUBLE_COL_NAME, null);
    assertEquals(DOUBLE_VAL_2, test, 0.00001);
  }

  @Test
  public void testInteger() {
    Integer test = ResultReader.getInteger(result, CF1_NAME, INT_COL_NAME, null);
    assertEquals(INT_VAL_1, test.intValue());

    test = ResultReader.getInteger(result, CF1_NAME, "fake col", 123456);
    assertEquals(Integer.valueOf(123456), test);

    test = ResultReader.getInteger(result, CF2_NAME, INT_COL_NAME, null);
    assertEquals(INT_VAL_2, test.intValue());
  }

  @Test
  public void testLong() {
    Long test = ResultReader.getLong(result, CF1_NAME, LONG_COL_NAME, null);
    assertEquals(LONG_VAL_1, test.longValue());

    test = ResultReader.getLong(result, CF1_NAME, "fake col", 1234567890l);
    assertEquals(Long.valueOf(1234567890l), test);

    test = ResultReader.getLong(result, CF2_NAME, LONG_COL_NAME, null);
    assertEquals(LONG_VAL_2, test.longValue());
  }

  @Test
  public void testBytes() {
    byte[] test = ResultReader.getBytes(result, CF1_NAME, BYTES_COL_NAME, null);
    assertTrue(Arrays.equals(BYTES_VAL_1, test));

    test = ResultReader.getBytes(result, CF1_NAME, "fake col", Bytes.toBytes(1234567890l));
    assertTrue(Arrays.equals(Bytes.toBytes(1234567890l), test));

    test = ResultReader.getBytes(result, CF2_NAME, BYTES_COL_NAME, null);
    assertTrue(Arrays.equals(BYTES_VAL_2, test));
  }
}
