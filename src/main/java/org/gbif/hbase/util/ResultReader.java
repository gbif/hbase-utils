package org.gbif.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Static utility methods for reading (Java) typed fields from an HBase Result.
 */
public class ResultReader {

  /**
   * Should never be constructed.
   */
  private ResultReader() {
  }

  /**
   * Read the value of this cell and interpret as String.
   *
   * @param row          the HBase Result from which to read
   * @param columnFamily column family that holds the column
   * @param columnName   column or "qualifier"
   * @param defaultValue returned if value at columnName is null
   *
   * @return the value from the specified column, or defaultValue if it's null/doesn't exist
   */
  public static String getString(Result row, String columnFamily, String columnName, String defaultValue) {
    Cell raw = row.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
    return  (raw == null) ? defaultValue : Bytes.toString(raw.getValueArray());
  }

  /**
   * Read the value of this cell and interpret as Integer.
   *
   * @param row          the HBase Result from which to read
   * @param columnFamily column family that holds the column
   * @param columnName   column or "qualifier"
   * @param defaultValue returned if value at columnName is null
   *
   * @return the value from the specified column, or defaultValue if it's null/doesn't exist
   */
  public static Integer getInteger(Result row, String columnFamily, String columnName, Integer defaultValue) {
    Cell raw = row.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
    return (raw == null) ? defaultValue : Integer.valueOf(Bytes.toInt(raw.getValueArray()));
  }

  /**
   * Read the value of this cell and interpret as Long.
   *
   * @param row          the HBase Result from which to read
   * @param columnFamily column family that holds the column
   * @param columnName   column or "qualifier"
   * @param defaultValue returned if value at columnName is null
   *
   * @return the value from the specified column, or defaultValue if it's null/doesn't exist
   */
  public static Long getLong(Result row, String columnFamily, String columnName, Long defaultValue) {
    Cell raw = row.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
    return (raw == null) ? defaultValue : Long.valueOf(Bytes.toLong(raw.getValueArray()));
  }

  /**
   * Read the value of this cell and interpret as Float.
   *
   * @param row          the HBase Result from which to read
   * @param columnFamily column family that holds the column
   * @param columnName   column or "qualifier"
   * @param defaultValue returned if value at columnName is null
   *
   * @return the value from the specified column, or defaultValue if it's null/doesn't exist
   */
  public static Float getFloat(Result row, String columnFamily, String columnName, Float defaultValue) {
    Cell raw = row.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
    return (raw == null) ? defaultValue : Float.valueOf(Bytes.toFloat(raw.getValueArray()));  }

  /**
   * Read the value of this cell and interpret as Double.
   *
   * @param row          the HBase Result from which to read
   * @param columnFamily column family that holds the column
   * @param columnName   column or "qualifier"
   * @param defaultValue returned if value at columnName is null
   *
   * @return the value from the specified column, or defaultValue if it's null/doesn't exist
   */
  public static Double getDouble(Result row, String columnFamily, String columnName, Double defaultValue) {
    Cell raw = row.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
    return (raw == null) ? defaultValue : Double.valueOf(Bytes.toDouble(raw.getValueArray()));
  }

  /**
   * Read the value of this cell and return it uninterpreted as byte[].
   *
   * @param row          the HBase Result from which to read
   * @param columnFamily column family that holds the column
   * @param columnName   column or "qualifier"
   * @param defaultValue returned if value at columnName is null
   *
   * @return the value from the specified column, or defaultValue if it's null/doesn't exist
   */
  public static byte[] getBytes(Result row, String columnFamily, String columnName, byte[] defaultValue) {
    Cell raw = row.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
    return (raw == null) ? defaultValue : raw.getValueArray();
  }

  // TODO: @nullable
  public static Long getTimestamp(Result row, String columnFamily, String columnName) {
    Cell raw = row.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
    return (raw == null) ? null : raw.getTimestamp();
  }
}
