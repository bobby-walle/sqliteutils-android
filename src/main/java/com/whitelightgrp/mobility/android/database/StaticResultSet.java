package com.whitelightgrp.mobility.android.database;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;

import android.database.Cursor;
import android.util.Base64;

/**
 * Stores and provides read-only access to query results in a static manner.
 *
 * @author Justin Rohde, WhiteLight Group
 */
public class StaticResultSet {
    /**
     * The number of columns in the result set.
     */
    private final int columnCount;
    /**
     * The number of rows in the result set.
     */
    private final int rowCount;
    /**
     * Map of column indices to column names for quick access.
     */
    private final HashMap<String, Integer> columnIndices = new HashMap<String, Integer>();
    /**
     * A list of metadata about the columns in the returned result set.
     */
    private final ArrayList<Column> columns;

    /**
     * @return The count of rows in the result set.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * @return The count of columns in the result set.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     * A list of rows constructed from the returned result set.
     */
    private final ArrayList<Row> rows;

    /**
     * Constructor.
     *
     * @param cursor The {@link android.database.Cursor} which provides access to the query results.
     */
    public StaticResultSet(Cursor cursor) {
        if (cursor == null) {
            rowCount = columnCount = 0;
            rows = new ArrayList<Row>();
            columns = new ArrayList<Column>();
            return;
        }

        // Get dimensions and allocate memory
        rowCount = cursor.getCount();
        columnCount = cursor.getColumnCount();
        rows = new ArrayList<Row>(rowCount);
        columns = new ArrayList<Column>(columnCount);

        int[] columnWidths = new int[columnCount];

        // Put data and calculate column widths
        for (int i = 0; i < rowCount; i++) {
            if (cursor.moveToPosition(i)) {
                Object[] values = new Object[columnCount];
                for (int j = 0; j < columnCount; j++) {
                    Object result;
                    if (cursor.getType(j) == Cursor.FIELD_TYPE_BLOB) {
                        result = cursor.getBlob(j);
                    }
                    else {
                        result = cursor.getString(j);
                    }
                    values[j] = result;
                    int length;
                    if (result == null) {
                        length = 0;
                    }
                    else if (result instanceof byte[]) {
                        length = humanReadableByteCount(((byte[]) result).length, true).length();
                    }
                    else {
                        length = ((String)result).length();
                    }
                    columnWidths[j] = Math.max(columnWidths[j], length);
                }
                rows.add(new Row(i, values));
            }
        }

        // Store column metadata, and put indices into map by column name
        for (int i = 0; i < columnCount; i++) {
            String columnName = cursor.getColumnName(i);
            int type = rowCount == 0? Cursor.FIELD_TYPE_STRING : cursor.getType(i);
            columns.add(new Column(i, columnName, Math.max(columnName.length(), columnWidths[i]), type));
            columnIndices.put(columnName, i);
        }
    }

    /**
     * @return Whether there are any rows in the result set.
     */
    public boolean isEmpty() {
        return rowCount == 0;
    }

    /**
     * @return The list of {@link Row}s constructed from the query results.
     */
    public ArrayList<Row> getRows() {
        return rows;
    }

    /**
     * @param index The index of the {@link Row} to retrieve.
     * @return The {@link Row} at the specified index.
     */
    public Row getRowAt(int index) {
        return rows.get(index);
    }

    /**
     * Get the index of the column with the specified name.
     *
     * @param name The name of the column.
     * @return The index of the column with the specified name, or {@code null} if no mapping found.
     */
    private Integer getColumnIndex(String name) {
        return columnIndices.get(name);
    }

    /**
     * Get the name of the column at the specified index.
     *
     * @param index The index of the column.
     * @return The name of the column, or {@code null} if not found.
     */
    public String getColumnNameAt(int index) {
        return index >= 0 && index < columns.size() ? columns.get(index).getName() : null;
    }

    /**
     * Get the type of the column at the specified index.
     */
    public int getColumnTypeAt(int index) {
        return index > 0 && index < columns.size() ? columns.get(index).getType() : -1;
    }

    /**
     * Convert results from a single column to a list.
     *
     * @param columnIndex The 0-based index of the column to convert.
     * @return The list of all items in the specified column.
     */
    public ArrayList<String> getColumnValues(int columnIndex) {
        ArrayList<String> list = new ArrayList<String>();
        for (int i = 0; i < rowCount; i++) {
            list.add(getRowAt(i).getString(columnIndex));
        }
        return list;
    }

    /**
     * Convert results from a single column to a list.
     *
     * @param columnName The name of the column to convert.
     * @return The list of all items in the specified column.
     */
    public ArrayList<String> getColumnValues(String columnName) {
        ArrayList<String> list = new ArrayList<String>();
        for (int i = 0; i < rowCount; i++) {
            list.add(getRowAt(i).getString(columnName));
        }
        return list;
    }

    /**
     * Return the column width at the specified index.
     *
     * @param index The index of the column.
     * @return The column width in characters.
     */
    public int getColumnWidthAt(int index) {
        return index >= 0 && index < columns.size() ? columns.get(index).getWidth() : 0;
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    /**
     * Metadata about a column.
     */
    public class Column {
        /**
         * Column number.
         */
        private final int number;
        /**
         * Column name.
         */
        private final String name;
        /**
         * Maximum number of characters in this column for all rows in the result set.
         */
        private final int width;
        /**
         * Column type.
         */
        private final int type;

        /**
         * Constructor.
         *
         * @param number The column number.
         * @param name The column name.
         * @param width The maximum number of characters in this column for all rows in the result set.
         * @param type The column type.
         */
        public Column(int number, String name, int width, int type) {
            this.number = number;
            this.name = name;
            this.width = width;
            this.type = type;
        }

        /**
         * @return The column number.
         */
        public int getColumnNumber() {
            return number;
        }

        /**
         * @return The column name.
         */
        public String getName() {
            return name;
        }

        /**
         * @return The maximum character width in this column for all rows in the result set.
         */
        public int getWidth() {
            return width;
        }

        public int getType() {
            return type;
        }
    }

    /**
     * Stores a single result row.
     */
    public class Row {
        /**
         * The row number.
         */
        private final int rowNumber;
        /**
         * The values in the row.
         */
        private final Object[] values;

        /**
         * Constructor.
         *
         * @param rowNumber The row number.
         * @param values The values obtained from the result set for this row.
         */
        private Row(int rowNumber, Object[] values) {
            this.rowNumber = rowNumber;
            this.values = values;
        }

        /**
         * Returns a {@link String} result.
         *
         * If this is a BLOB column, the byte array will first be converted to a base 64-encoded string.
         *
         * @param column The 0-based column index.
         * @return The {@link String} value, or {@code null} if no result.
         */
        public String getString(int column) {
            Object value = values[column];
            if (value instanceof byte[]) {
                return Base64.encodeToString((byte[])value, Base64.DEFAULT);
            }
            else {
                return column >= 0 && column < values.length ? (String)values[column] : null;
            }
        }

        /**
         * Returns a byte array.
         *
         * @param column The 0-based column index.
         * @return The {@link String} value, or {@code null} if the value is not a byte array.
         */
        public byte[] getByteArray(int column) {
            Object value = values[column];
            if (value instanceof byte[]) {
                return (byte[]) value;
            }
            else {
                return null;
            }
        }

        /**
         * Returns a {@link String} result, or a default value if the result is {@code null}.
         *
         * @param column The 0-based column index.
         * @param defaultValue The value to return if the result is {@code null}.
         * @return The result if non-null, or the default value.
         */
        public String getString(int column, String defaultValue) {
            String result = getString(column);
            return result == null ? defaultValue : result;
        }

        /**
         * Returns a {@link String} result by column name.
         *
         * @param columnName The name of the column from which to retrieve the result.
         * @return The result value, or {@code null} if no result.
         */
        public String getString(String columnName) {
            return getString(getColumnIndex(columnName));
        }

        /**
         * Returns a {@link String} result by column name, or a default value if the result is {@code null}.
         *
         * @param columnName The name of the column from which to retrieve the result.
         * @param defaultValue The value to return if the result if {@code null}.
         * @return The result value if non-null, or the default value.
         */
        public String getString(String columnName, String defaultValue) {
            return getString(getColumnIndex(columnName), defaultValue);
        }

        /**
         * Returns a {@link BigDecimal} result.
         *
         * @param column The 0-based column index.
         * @return The {@link BigDecimal} result if non-null, or {@code null} if invalid, out of range, or no results.
         */
        public BigDecimal getBigDecimal(int column) {
            try {
                String value = StringUtils.trim(getString(column));
                return value == null ? null : new BigDecimal(value);
            }
            catch (NumberFormatException e) {
                return null;
            }
        }

        /**
         * Returns a {@link BigDecimal} result, or a default value if the result is {@code null}.
         *
         * @param column The 0-based column index.
         * @param defaultValue The value to return if the result is {@code null}.
         * @return The {@link BigDecimal} result if non-null, or the default value.
         */
        public BigDecimal getBigDecimal(int column, BigDecimal defaultValue) {
            BigDecimal result = getBigDecimal(column);
            return result == null ? defaultValue : result;
        }

        /**
         * Returns a {@link BigDecimal} from the results by column name.
         *
         * @param columnName The name of the column from which to extract.
         * @return The {@link BigDecimal} result, or {@code null} if invalid, out of range, or no results.
         */
        public BigDecimal getBigDecimal(String columnName) {
            return getBigDecimal(getColumnIndex(columnName));
        }

        /**
         * Returns a {@link BigDecimal} result by column name, or a default value if the result is {@code null}.
         *
         * @param columnName The name of the column from which to extract.
         * @param defaultValue The value to return if the result is {@code null}.
         * @return The {@link BigDecimal} result if non-null, or the default value.
         */
        public BigDecimal getBigDecimal(String columnName, BigDecimal defaultValue) {
            return getBigDecimal(getColumnIndex(columnName), defaultValue);
        }

        /**
         * Returns an {@link Integer} result.
         *
         * @param column The 0-based column index.
         * @return The {@link Integer} value at the specified location, or {@code null} if invalid, out of range, or no results.
         */
        public Integer getInt(int column) {
            try {
                String value = StringUtils.trim(getString(column));
                return value == null ? null : Integer.parseInt(value);
            }
            catch (NumberFormatException e) {
                return null;
            }
        }

        /**
         * Returns an {@code int} result, or a default value if the result is {@code null}.
         *
         * @param column The 0-based column index.
         * @param defaultValue The value to return if the result is {@code null}.
         * @return The {@code int} result, or the default value.
         */
        public int getInt(int column, int defaultValue) {
            Integer result = getInt(column);
            return result == null ? defaultValue : result;
        }

        /**
         * Returns an {@link Integer} result by column name.
         *
         * @param columnName The name of the column from which to extract.
         * @return The {@link Integer} result, or {@code null} if invalid, out of range, or no results.
         */
        public Integer getInt(String columnName) {
            return getInt(getColumnIndex(columnName));
        }

        /**
         * Returns an {@code int} result by colum name, or a default value if the result is {@code null}.
         *
         * @param columnName The name of the column from which to extract.
         * @param defaultValue The value to return if the result is {@code null}.
         * @return The {@code int} value at the specified location, or {@code null} if invalid, out of range, or no results.
         */
        public int getInt(String columnName, int defaultValue) {
            return getInt(getColumnIndex(columnName), defaultValue);
        }

        /**
         * Returns a {@link Long} result.
         *
         * @param column The 0-based column index.
         * @return The {@link Long} result, or {@code null} if invalid, out of range, or no results.
         */
        public Long getLong(int column) {
            try {
                String value = StringUtils.trim(getString(column));
                return value == null ? null : Long.parseLong(value);
            }
            catch (NumberFormatException e) {
                return null;
            }
        }

        /**
         * Returns a {@code long} result, or a default value if the result is {@code null}.
         *
         * @param column The 0-based column index.
         * @param defaultValue The value to return if the result is {@code null}.
         * @return The {@code long} result if non-null, or the default value.
         */
        public long getLong(int column, long defaultValue) {
            Long result = getLong(column);
            return result == null ? defaultValue : result;
        }

        /**
         * Returns a {@link Long} result by column name.
         *
         * @param columnName The name of the column from which to extract.
         * @return The {@link Long} result, or {@code null} if invalid, out of range, or no results.
         */
        public Long getLong(String columnName) {
            return getLong(getColumnIndex(columnName));
        }

        /**
         * Returns a {@link Long} result by column name, or a default value if the result is {@code null}.
         *
         * @param columnName The name of the column from which to extract.
         * @param defaultValue The value to return if the result is {@code null}.
         * @return The {@link Long} result, or the default value.
         */
        public long getLong(String columnName, long defaultValue) {
            return getLong(getColumnIndex(columnName), defaultValue);
        }

        /**
         * @return The row number.
         */
        public int getRowNumber() {
            return rowNumber;
        }
    }

    /**
     * Return a human-readable representation of a byte length.
     *
     * @param bytes The number of bytes.
     * @param decimal {@code true} if decimal (SI) should be used, or {@code false} for binary.
     * @return The human-readable string representation.
     */
    public static String humanReadableByteCount(long bytes, boolean decimal) {
        int unit = decimal ? 1000 : 1024;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (decimal ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (decimal ? "" : "i");
        return String.format(Locale.US, "%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}