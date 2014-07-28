package com.whitelightgrp.mobility.android.database;

import java.math.BigDecimal;
import java.util.ArrayList;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;

/**
 * Provides several useful methods to simplify working with a SQLiteDatabase.
 *
 * @author Justin Rohde, WhiteLight Group
 */
public class SQLiteUtils {
    /**
     * Log tag.
     */
    private final static String LOG_TAG = SQLiteUtils.class.getSimpleName();

    /**
     * Insert values into a table, additionally updating the TIME_STAMP field with the current time.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table into which to insert records.
     * @param values A map of column-value pairs to insert.
     * @return The row ID of the newly inserted row, or -1 if an error occurred.
     */
    public static long insertWithTimestampUpdate(Context context, String tableName, ContentValues values) {
        // Update the timestamp of the record
        values.put("TIME_STAMP", System.currentTimeMillis());

        // Insert the record
        SQLiteDatabase db = OpenHelper.getDatabase(context);
        return db.insert(tableName, null, values);
    }

    /**
     * Insert multiple records into a table in sequence. <p> If a single insert fails, the entire operation will be rolled back. </p>
     *
     * @param context The {@link android.content.Context} used to open the database.
     * @param tableName The name of the table into which to insert records.
     * @param values A list of maps of column-value pairs to insert.
     * @return {@code true} if all columns were inserted successfully, {@code false} otherwise.
     */
    public static boolean insertMultiple(Context context, String tableName, ArrayList<ContentValues> values) {
        SQLiteDatabase db = OpenHelper.getDatabase(context);
        try {
            // Begin transaction
            db.beginTransaction();
            for (ContentValues cv : values) {
                if (db.insert(tableName, null, cv) == -1) {
                    return false;
                }
            }

            // Commit the data
            db.setTransactionSuccessful();
            return true;
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return false;
        }
        finally {
            // End the transaction, whether the data was committed or not.
            db.endTransaction();
        }
    }

    /**
     * Copy all records from the source table into the destination table, optionally filtering the source records. <p> If the primary keys exist for a
     * record, the existing record is replaced. </p>
     *
     * @param db The database containing the source and destination table.
     * @param destinationTable The destination table name.
     * @param sourceTable The source table name.
     * @param whereClause Optional SQL {@code WHERE} clause to apply to the source record set. Use {@code null} to select all records in the source
     * table.
     * @param whereArgs Arguments for the optional SQL {@code WHERE} clause, or {@code null} if there are no arguments.
     * @param conflictAlgorithm The algorithm to use on conflict.  One of {@link SQLiteDatabase#CONFLICT_ROLLBACK}, {@link
     * SQLiteDatabase#CONFLICT_REPLACE}, {@link SQLiteDatabase#CONFLICT_FAIL}, {@link SQLiteDatabase#CONFLICT_ABORT}, {@link
     * SQLiteDatabase#CONFLICT_NONE}, {@link SQLiteDatabase#CONFLICT_IGNORE}.
     * @return The number of records copied, or -1 if an error occurred.
     */
    public static long copyRecords(
            SQLiteDatabase db,
            String destinationTable,
            String sourceTable,
            String whereClause,
            String[] whereArgs,
            int conflictAlgorithm
    ) {
        // Build list of column names
        StringBuilder sb = new StringBuilder();
        Cursor cursor = db.query(sourceTable, null, null, null, null, null, null, "1");
        for (int i = 0; i < cursor.getColumnCount(); i++) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(cursor.getColumnName(i));
        }

        long count;
        try {
            count = DatabaseUtils.queryNumEntries(db, sourceTable, whereClause, whereArgs);
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            count = 0;
        }

        String conflictAlgorithmText;
        switch (conflictAlgorithm) {
            case SQLiteDatabase.CONFLICT_IGNORE:
                conflictAlgorithmText = "OR IGNORE ";
                break;
            case SQLiteDatabase.CONFLICT_ABORT:
                conflictAlgorithmText = "OR ABORT ";
                break;
            case SQLiteDatabase.CONFLICT_FAIL:
                conflictAlgorithmText = "OR FAIL ";
                break;
            case SQLiteDatabase.CONFLICT_REPLACE:
                conflictAlgorithmText = "OR REPLACE ";
                break;
            case SQLiteDatabase.CONFLICT_ROLLBACK:
                conflictAlgorithmText = "OR ROLLBACK ";
                break;
            case SQLiteDatabase.CONFLICT_NONE:
            default:
                conflictAlgorithmText = "";
                break;
        }

        String sql = String.format(
                "INSERT " + conflictAlgorithmText + "INTO %s(%s) SELECT %s FROM %s",
                destinationTable,
                sb,
                sb,
                sourceTable
        );

        if (whereClause != null) {
            sql += " WHERE " + whereClause;
        }

        if (safeExecSql(db, sql, whereArgs)) {
            return count;
        }
        else {
            return -1;
        }
    }

    /**
     * Perform a SQL query to obtain a result set.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param columns A list of which columns to return. Passing null will return all columns, which is discouraged to prevent reading data from
     * storage that isn't going to be used.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The {@link StaticResultSet} containing the result values. If an error occurred, the {@link StaticResultSet#rowCount} will be {@code 0}.
     */
    public static StaticResultSet query(Context context, String tableName, String[] columns, String selection, String[] selectionArgs) {
        return query(context, tableName, columns, selection, selectionArgs, null, null, null, null);
    }

    /**
     * Perform a SQL query to obtain a result set.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param columns A list of which columns to return. Passing null will return all columns, which is discouraged to prevent reading data from
     * storage that isn't going to be used.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL GROUP BY clause (excluding the GROUP BY itself). Passing null will
     * cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor, if row grouping is being used, formatted as an SQL HAVING clause
     * (excluding the HAVING itself). Passing null will cause all row groups to be included, and is required when row grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause (excluding the ORDER BY itself). Passing null will use the default
     * sort order, which may be unordered.
     * @param limit Limits the number of rows returned by the query, formatted as LIMIT clause. Passing null denotes no LIMIT clause.
     * @return The {@link StaticResultSet} containing the result values, or an empty {@link StaticResultSet} if an error occurred.
     */
    public static StaticResultSet query(
            Context context,
            String tableName,
            String[] columns,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy,
            String limit
    ) {
        Cursor cursor = null;
        try {
            SQLiteDatabase db = OpenHelper.getDatabase(context);
            cursor = db.query(tableName, columns, selection, selectionArgs, groupBy, having, orderBy, limit);

            // Create the result set
            return new StaticResultSet(cursor);
        }
        catch (SQLiteException e) {
            // Return an empty result set
            return new StaticResultSet(null);
        }
        finally {
            // Clean up
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Perform a SQL query, placing the results into a list of maps of column-value pairs.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param columns A list of which columns to return. Passing null will return all columns, which is discouraged to prevent reading data from
     * storage that isn't going to be used.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL GROUP BY clause (excluding the GROUP BY itself). Passing null will
     * cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor, if row grouping is being used, formatted as an SQL HAVING clause
     * (excluding the HAVING itself). Passing null will cause all row groups to be included, and is required when row grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause (excluding the ORDER BY itself). Passing null will use the default
     * sort order, which may be unordered.
     * @param limit Limits the number of rows returned by the query, formatted as LIMIT clause. Passing null denotes no LIMIT clause.
     * @return A map of column-value pairs containing the results, or null of an error occurred.
     */
    public static ArrayList<ContentValues> queryAsContentValues(
            Context context,
            String tableName,
            String[] columns,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy,
            String limit
    ) {
        Cursor cursor = null;
        try {
            // Perform the query
            SQLiteDatabase db = OpenHelper.getDatabase(context);
            cursor = db.query(tableName, columns, selection, selectionArgs, groupBy, having, orderBy, limit);

            ArrayList<ContentValues> values = new ArrayList<ContentValues>();
            while (cursor.moveToNext()) {
                // Convert row to ContentValues and add to list
                ContentValues rowValues = new ContentValues();
                android.database.DatabaseUtils.cursorRowToContentValues(cursor, rowValues);
                values.add(rowValues);
            }
            return values;
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            // Clean up
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Convenience method to query for a single array of {@code byte}.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The single {@code byte[]} result. Set to {@code null} if an error occurred.
     */
    public static byte[] queryForByteArray(Context context, String tableName, String column, String selection, String[] selectionArgs) {
        return (byte[]) queryForSingleValue(context, tableName, column, selection, selectionArgs, null, null, null, DataType.BLOB);
    }

    /**
     * Return the count of all records in a table.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The name of the table to query.
     * @return The number of records in the table.
     */
    public static int queryForCount(Context context, String tableName) {
        return queryForCount(context, tableName, null, null);
    }

    /**
     * Return the count of all records in a table matching a selection.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The number of records in the selection, or 0 if an error occurred.
     */
    public static int queryForCount(Context context, String tableName, String selection, String[] selectionArgs) {
        SQLiteDatabase db = OpenHelper.getDatabase(context);
        try {
            return (int) DatabaseUtils.queryNumEntries(db, tableName, selection, selectionArgs);
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Convenience method to return a single {@link Long}.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The single {@link Long} result. Set to {@code null} if an error occurred.
     */
    public static Long queryForLong(Context context, String tableName, String column, String selection, String[] selectionArgs) {
        return (Long) queryForSingleValue(context, tableName, column, selection, selectionArgs, null, null, null, DataType.LONG);
    }

    /**
     * Convenience method to return a single {@code long}, or a default value if no result is found.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param defaultValue The value to return if there query result is {@code null}.
     * @return The single {@link Long} result. Set to {@code null} if an error occurred.
     */
    public static long queryForLongWithDefault(
            Context context,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            long defaultValue
    ) {
        Long result = (Long) queryForSingleValue(context, tableName, column, selection, selectionArgs, null, null, null, DataType.LONG);
        return result == null ? defaultValue : result;
    }

    /**
     * Convenience method to return a single {@link Long}.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL GROUP BY clause (excluding the GROUP BY itself). Passing null will
     * cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor, if row grouping is being used, formatted as an SQL HAVING clause
     * (excluding the HAVING itself). Passing null will cause all row groups to be included, and is required when row grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause (excluding the ORDER BY itself). Passing null will use the default
     * sort order, which may be unordered.
     * @return The single {@link Long} result. Set to {@code null} if an error occurred.
     */
    public static Long queryForLong(
            Context context,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy
    ) {
        return (Long) queryForSingleValue(context, tableName, column, selection, selectionArgs, groupBy, having, orderBy, DataType.LONG);
    }

    /**
     * Convenience method to return a single {@link java.math.BigDecimal}, or a default value is no result is found.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param defaultValue The value to return if the query result is {@code null}.
     * @return The single {@link java.math.BigDecimal} result. Set to {@code null} if an error occurred.
     */
    public static BigDecimal queryForBigDecimalWithDefault(
            Context context,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            BigDecimal defaultValue
    ) {
        String value = queryForString(context, tableName, column, selection, selectionArgs, null, null, null);
        return value == null ? defaultValue : new BigDecimal(value);
    }

    /**
     * Convenience method to return a single {@link java.math.BigDecimal}, or a default value if no result is found.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL GROUP BY clause (excluding the GROUP BY itself). Passing null will
     * cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor, if row grouping is being used, formatted as an SQL HAVING clause
     * (excluding the HAVING itself). Passing null will cause all row groups to be included, and is required when row grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause (excluding the ORDER BY itself). Passing null will use the default
     * sort order, which may be unordered.
     * @param defaultValue The value to return if the query result is {@code null}.
     * @return The single {@link BigDecimal} result. Set to {@code null} if an error occurred.
     */
    public static BigDecimal queryForBigDecimalWithDefault(
            Context context,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy,
            BigDecimal defaultValue
    ) {
        String value = queryForString(context, tableName, column, selection, selectionArgs, groupBy, having, orderBy);
        return value == null ? defaultValue : new BigDecimal(value);
    }

    /**
     * Convenience method to return a single {@link Integer}.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The single {@link Integer} result. Set to {@code null} if an error occurred.
     */
    public static Integer queryForInt(Context context, String tableName, String column, String selection, String[] selectionArgs) {
        return (Integer) queryForSingleValue(context, tableName, column, selection, selectionArgs, null, null, null, DataType.INTEGER);
    }

    /**
     * Convenience method to return a single {@code int}, or a default value if no result is found.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param defaultValue The value to return if the query result is {@code null}.
     * @return The single {@link Integer} result. Set to {@code null} if an error occurred.
     */
    public static int queryForIntWithDefault(
            Context context,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            int defaultValue
    ) {
        Integer result = (Integer) queryForSingleValue(context, tableName, column, selection, selectionArgs, null, null, null, DataType.INTEGER);
        return result == null ? defaultValue : result;
    }

    /**
     * Perform a query to return a single {@link Object}.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL GROUP BY clause (excluding the GROUP BY itself). Passing null will
     * cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor, if row grouping is being used, formatted as an SQL HAVING clause
     * (excluding the HAVING itself). Passing null will cause all row groups to be included, and is required when row grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause (excluding the ORDER BY itself). Passing null will use the default
     * sort order, which may be unordered.
     * @param type The type of result to get from the cursor.
     * @return The single {@link Object} result. Set to {@code null} if an error occurred.
     */
    private static Object queryForSingleValue(
            Context context,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy,
            DataType type
    ) {
        Cursor cursor = null;
        try {
            // Perform the query
            SQLiteDatabase db = OpenHelper.getDatabase(context);
            cursor = db.query(tableName, new String[] { column }, selection, selectionArgs, groupBy, having, orderBy, "1");

            if (cursor.moveToFirst()) {
                // Return the result as the specified type
                switch (type) {
                    case STRING:
                        return cursor.getString(0);
                    case INTEGER:
                        return cursor.getInt(0);
                    case LONG:
                        return cursor.getLong(0);
                    case BLOB:
                        return cursor.getBlob(0);
                    default:
                        return cursor.getString(0);
                }
            }
            else {
                return null;
            }
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            // Clean up
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Returns a single {@code String} from a query.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The String representation of the result. Set to {@code null} if an error occurred.
     */
    public static String queryForString(Context context, String tableName, String column, String selection, String[] selectionArgs) {
        return queryForString(context, tableName, column, selection, selectionArgs, null, null, null);
    }

    /**
     * Returns a single {@code String}.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param groupBy A filter declaring how to group rows, formatted as an SQL GROUP BY clause (excluding the GROUP BY itself). Passing null will
     * cause the rows to not be grouped.
     * @param having A filter declare which row groups to include in the cursor, if row grouping is being used, formatted as an SQL HAVING clause
     * (excluding the HAVING itself). Passing null will cause all row groups to be included, and is required when row grouping is not being used.
     * @param orderBy How to order the rows, formatted as an SQL ORDER BY clause (excluding the ORDER BY itself). Passing null will use the default
     * sort order, which may be unordered.
     * @return The String representation of the result. Set to {@code null} if an error occurred.
     */
    public static String queryForString(
            Context context,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy
    ) {
        return (String) queryForSingleValue(context, tableName, column, selection, selectionArgs, groupBy, having, orderBy, DataType.STRING);
    }

    /**
     * Execute a single SQL statement, eating any exception that occurs.
     *
     * @param db The database against which to execute the query.
     * @param sql The SQL statement to execute.
     * @param bindArgs An array of arguments to the query.
     * @return {@code true} if successful, {@code false} otherwise.
     */
    public static boolean safeExecSql(SQLiteDatabase db, String sql, Object[] bindArgs) {
        try {
            if (bindArgs == null) {
                db.execSQL(sql);
            }
            else {
                db.execSQL(sql, bindArgs);
            }
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Execute a single SQL statement, eating any exception that occurs.
     *
     * @param db The database against which to execute the query.
     * @param sql The SQL statement to execute.
     * @return {@code true} if successful, {@code false} otherwise.
     */
    public static boolean safeExecSql(SQLiteDatabase db, String sql) {
        try {
            db.execSQL(sql);
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Convenience method to perform a raw query with no selection arguments to bind.
     *
     * @param context The {@link Context} used to open the database.
     * @param sql The SQL query to execute.
     * @return A {@link StaticResultSet} containing the query results.
     */
    public static StaticResultSet rawQuery(Context context, String sql) {
        return rawQuery(context, sql, null);
    }

    /**
     * Convenience method to perform a raw query with no selection arguments to bind.
     *
     * @param context The {@link Context} used to open the database.
     * @param sql The SQL query to execute.
     * @param selectionArgs An optional array of parameters to bind to the query.
     * @return A {@link StaticResultSet} containing the query results.
     */
    public static StaticResultSet rawQuery(Context context, String sql, String[] selectionArgs) {
        SQLiteDatabase db = OpenHelper.getDatabase(context);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, selectionArgs);
            return new StaticResultSet(cursor);
        }
        catch (SQLiteException ex) {
            ex.printStackTrace();

            // Return an empty result set
            return new StaticResultSet(null);
        }
        finally {
            // Clean up
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Return a single result from a query.
     *
     * @param context The {@link Context} used to open the database.
     * @param sql The SQL query to execute.
     * @param selectionArgs An optional array of selection arguments.
     * @param type The value type to return.
     * @return The value of the result. Set to {@code null} if no result found or an error occurred.
     */
    private static Object rawQueryForSingleValue(Context context, String sql, String[] selectionArgs, DataType type) {
        // Fetch only the first row
        if (sql != null && !sql.endsWith(" LIMIT 1")) {
            sql += " LIMIT 1";
        }

        SQLiteDatabase db = OpenHelper.getDatabase(context);
        Cursor cursor = null;
        try {
            // Perform the query
            cursor = db.rawQuery(sql, selectionArgs);
            if (cursor.moveToFirst()) {
                // Return result based on type specified
                switch (type) {
                    case STRING:
                        return cursor.getString(0);
                    case INTEGER:
                        return cursor.getInt(0);
                    case LONG:
                        return cursor.getLong(0);
                    case BLOB:
                        return cursor.getBlob(0);
                    default:
                        return cursor.getString(0);
                }
            }
            else {
                return null;
            }
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            // Clean up
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Export a table to the specified database. If the database does not exist, it will be created.
     *
     * @param context The {@link Context} used to open the database.
     * @param tableName The table to export.
     * @param absolutePath The database file path.
     * @return {@code true} if successful, {@code false} otherwise.
     */
    public static boolean exportTable(Context context, String tableName, String absolutePath) {
        // Make sure the database file exists
        try {
            SQLiteDatabase db = SQLiteDatabase.openOrCreateDatabase(absolutePath, null);
            db.close();
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return false;
        }

        SQLiteDatabase db = OpenHelper.getDatabase(context);
        safeExecSql(db, "DETACH DATABASE Export");
        if (!safeExecSql(db, "ATTACH DATABASE '" + absolutePath + "' AS Export")) {
            return false;
        }

        // Begin a new transaction
        db.beginTransaction();

        // Drop any existing table with the export table name
        String exportTableName = "Export." + tableName;
        db.execSQL("DROP TABLE IF EXISTS " + exportTableName);

        // Create the new table
        try {
            String sql = DatabaseUtils.stringForQuery(db, "SELECT sql FROM sqlite_master WHERE type='table' AND name='" + tableName + "'", null);
            sql = sql.replaceFirst(tableName, exportTableName);
            sql = "CREATE TABLE Export.MyTable(Id INTEGER PRIMARY KEY)";
            db.execSQL(sql);
            Cursor cursor = db.query("sqlite_master", new String[] { "name"}, "type='table'", null, null, null, null, "1");
            DatabaseUtils.dumpCursor(cursor);
            cursor.close();
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            db.endTransaction();
            return false;
        }

        // Copy all data from the internal database to the external database
        if (copyRecords(db, exportTableName, tableName, null, null, SQLiteDatabase.CONFLICT_REPLACE) == -1) {
            db.endTransaction();
            return false;
        }

        // Commit changes
        db.setTransactionSuccessful();
        db.endTransaction();

        // Detach the external database
        safeExecSql(db, "DETACH DATABASE Export");

        return true;
    }

    /**
     * Import tables from a database file at {@code absolutePath}.
     *
     * @param context The {@link android.content.Context} used to open the database.
     * @param absolutePath The absolute path of the database file.
     * @param tableName The name of the table to import.
     * @return {@code true} if successful, {@code false} upon failure or if the file is not valid.
     */
    public static boolean importTable(Context context, String absolutePath, String tableName) {
        // Open the database file to validate
        try {
            SQLiteDatabase db = SQLiteDatabase.openDatabase(absolutePath, null, SQLiteDatabase.OPEN_READONLY);
            db.close();
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return false;
        }

        // Attach the external database so we can include it in SQL query
        SQLiteDatabase db = OpenHelper.getDatabase(context);
        safeExecSql(db, "DETACH DATABASE Import");
        if (!safeExecSql(db, "ATTACH DATABASE '" + absolutePath + "' AS Import")) {
            return false;
        }

        // Begin a new transaction
        db.beginTransaction();

        // Remove any existing table with this name
        db.execSQL("DROP TABLE IF EXISTS " + tableName);

        // Create the new table
        String sql;
        try {
            sql = DatabaseUtils.stringForQuery(db, "SELECT sql FROM Import.sqlite_master WHERE type='table' AND name='" + tableName + "'", null);
            db.execSQL(sql);
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            db.endTransaction();
            return false;
        }

        // Copy all data from the external database to the internal database
        if (copyRecords(db, tableName, "Import." + tableName, null, null, SQLiteDatabase.CONFLICT_REPLACE) == -1) {
            db.endTransaction();
            return false;
        }

        // Commit the changes
        db.setTransactionSuccessful();
        db.endTransaction();

        // Detach the external database
        safeExecSql(db, "DETACH DATABASE External");

        return true;
    }

    /**
     * Get a list of all tables in the specified database file (excluding sqlite_master and android_metadata).
     *
     * @param db The database containing the tables to list.
     * @return The list of database table names, or {@code null} if the file does not exist.
     */
    public static ArrayList<String> listTables(SQLiteDatabase db) {
        String query = "type='table' AND name NOT IN('sqlite_master', 'android_metadata')";
        Cursor cursor = db.query("name", null, query, null, null, null, null);
        ArrayList<String> tableList = new ArrayList<String>();
        while (cursor.moveToNext()) {
            tableList.add(cursor.getString(cursor.getColumnIndex("name")));
        }
        cursor.close();
        return tableList;
    }

    /**
     * Returns a query with bind arguments filled in as literal text.
     *
     * @param sql The raw SQL text.
     * @param bindArgs The bind arguments, which will replace ?s in the raw SQL text.
     * @return The text with arguments filled as literal text.
     */
    private static String formatSql(String sql, Object[] bindArgs) {
        if (sql == null) {
            return null;
        }

        if (bindArgs != null) {
            for (Object bindArg : bindArgs) {
                sql = sql.replaceFirst("\\?", "[" + String.valueOf(bindArg) + "]");
            }
        }
        return sql;
    }

    /**
     * Return only the first row from a query.
     *
     * @param context The {@link Context} used to open the database.
     * @param table The table to query.
     * @param columns The columns to fetch, or {@code null} to fetch all columns.
     * @param selection The selection criteria.
     * @param selectionArgs Selection arguments.
     * @return A {@link StaticResultSet.Row} containing the results.
     */
    public static StaticResultSet.Row queryFirstRow(Context context, String table, String[] columns, String selection, String[] selectionArgs) {
        StaticResultSet resultSet = query(context, table, columns, selection, selectionArgs, null, null, null, "1");
        return resultSet.isEmpty() ? null : resultSet.getRowAt(0);
    }

    /**
     * Database-to-Java conversion types.
     */
    private static enum DataType {
        /**
         * Convert to {@link String}.
         */
        STRING,
        /**
         * Convert to {@link Integer}.
         */
        INTEGER,
        /**
         * Convert to {@link Long}.
         */
        LONG,
        /**
         * Convert to byte array.
         */
        BLOB
    }
}