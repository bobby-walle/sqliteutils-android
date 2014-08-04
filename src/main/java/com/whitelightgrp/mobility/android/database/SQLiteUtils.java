package com.whitelightgrp.mobility.android.database;

import java.util.ArrayList;
import java.util.Locale;

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
        cursor.close();

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
     * Convenience method to retrieve a single byte array.
     *
     * @param db The database to query.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The single {@code byte[]} result. Set to {@code null} if an error occurred.
     */
    public static byte[] safeQueryForByteArray(SQLiteDatabase db, String tableName, String column, String selection, String[] selectionArgs) {
        return safeQueryForByteArray(db, tableName, column, selection, selectionArgs, null, null, null);
    }

    /**
     * Convenience method to retrieve a single byte array.
     *
     * @param db The database to query.
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
     * @return The single {@code byte[]} result. Set to {@code null} if an error occurred.
     */
    public static byte[] safeQueryForByteArray(
            SQLiteDatabase db,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy
    ) {
        Cursor cursor = db.query(tableName, new String[] { column }, selection, selectionArgs, groupBy, having, orderBy, "1");
        try {
            return cursor.moveToFirst() ? cursor.getBlob(0) : null;
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Return the count of all records in a table matching a selection.
     *
     * @param db The database containing the table to query.
     * @param tableName The table name to compile the query against.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The number of records in the selection, or 0 if the table does not exist.
     */
    public static long safeQueryForCount(SQLiteDatabase db, String tableName, String selection, String[] selectionArgs) {
        try {
            return DatabaseUtils.queryNumEntries(db, tableName, selection, selectionArgs);
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Convenience method to return a single {@link Long}.
     *
     * @param db The database to query.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The single {@link Long} result. Set to {@code null} if an error occurred.
     */
    public static Long safeQueryForLong(SQLiteDatabase db, String tableName, String column, String selection, String[] selectionArgs) {
        return safeQueryForLong(db, tableName, column, selection, selectionArgs, null, null, null);
    }

    /**
     * Convenience method to return a single {@link Long}.
     *
     * @param db The database to query.
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
    public static Long safeQueryForLong(
            SQLiteDatabase db,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy
    ) {
        Cursor cursor = db.query(tableName, new String[] { column }, selection, selectionArgs, groupBy, having, orderBy, "1");
        try {
            return cursor.moveToFirst() ? cursor.getLong(0) : null;
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Convenience method to retrieve a single long value, or a default if the query returns no rows.
     *
     * @param db The database to query.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param defaultValue The value to return if there query result is {@code null}.
     * @return The single {@link Long} result. Set to {@code null} if an error occurred.
     */
    public static long queryForLong(
            SQLiteDatabase db,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            long defaultValue
    ) {
        return queryForLong(db, tableName, column, selection, selectionArgs, null, null, null, defaultValue);
    }

    /**
     * Convenience method to retrieve a single long value, or a default if the query returns no rows.
     *
     * @param db The database to query.
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
     * @param defaultValue The value to return if there query result is {@code null}.
     * @return The single {@link Long} result. Set to {@code null} if an error occurred.
     */
    public static long queryForLong(
            SQLiteDatabase db,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy,
            long defaultValue
    ) {
        Cursor cursor = db.query(tableName, new String[] { column }, selection, selectionArgs, groupBy, having, orderBy, "1");
        try {
            return cursor.moveToFirst() ? cursor.getLong(0) : defaultValue;
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return defaultValue;
        }
        finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Convenience method to return a single {@link Integer}.
     *
     * @param db The database to query.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The single {@link Integer} result. Set to {@code null} if an error occurred.
     */
    public static Integer safeQueryForInt(SQLiteDatabase db, String tableName, String column, String selection, String[] selectionArgs) {
        return safeQueryForInt(db, tableName, column, selection, selectionArgs, null, null, null);
    }

    /**
     * Convenience method to return a single {@link Integer}.
     *
     * @param db The database to query.
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
    public static Integer safeQueryForInt(
            SQLiteDatabase db,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy
    ) {
        Cursor cursor = db.query(tableName, new String[] { column }, selection, selectionArgs, groupBy, having, orderBy, "1");
        try {
            return cursor.moveToFirst() ? cursor.getInt(0) : null;
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Convenience method to return a single {@code int}, or a default value if no result is found.
     *
     * @param db The database to query.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @param defaultValue The value to return if the query result is {@code null}.
     * @return The single {@link Integer} result. Set to {@code null} if an error occurred.
     */
    public static int queryForInt(
            SQLiteDatabase db,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            int defaultValue
    ) {
        return queryForInt(db, tableName, column, selection, selectionArgs, null, null, null, defaultValue);
    }

    /**
     * Convenience method to return a single {@code int}, or a default value if no result is found.
     *
     * @param db The database to query.
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
     * @return The single {@link Integer} result. Set to {@code null} if an error occurred.
     */
    public static int queryForInt(
            SQLiteDatabase db,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy,
            int defaultValue
    ) {
        Cursor cursor = db.query(tableName, new String[] { column }, selection, selectionArgs, groupBy, having, orderBy, "1");
        try {
            return cursor.moveToFirst() ? cursor.getInt(0) : defaultValue;
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return defaultValue;
        }
        finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Returns a single {@code String} from a query.
     *
     * @param db The database to query.
     * @param tableName The table name to compile the query against.
     * @param column The column to return.
     * @param selection A filter declaring which rows to return, formatted as an SQL WHERE clause (excluding the WHERE itself). Passing null will
     * return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which will be replaced by the values from selectionArgs, in order that they appear in the
     * selection. The values will be bound as Strings.
     * @return The String representation of the result. Set to {@code null} if an error occurred.
     */
    public static String safeQueryForString(SQLiteDatabase db, String tableName, String column, String selection, String[] selectionArgs) {
        return safeQueryForString(db, tableName, column, selection, selectionArgs, null, null, null);
    }

    /**
     * Returns a single {@code String}.
     *
     * @param db The database to query.
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
    public static String safeQueryForString(
            SQLiteDatabase db,
            String tableName,
            String column,
            String selection,
            String[] selectionArgs,
            String groupBy,
            String having,
            String orderBy
    ) {
        Cursor cursor = db.query(tableName, new String[] { column }, selection, selectionArgs, groupBy, having, orderBy, "1");
        try {
            return cursor.moveToFirst() ? cursor.getString(0) : null;
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            if (cursor != null) {
                cursor.close();
            }
        }
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
            db.execSQL(sql);
            Cursor cursor = db.query("sqlite_master", new String[] { "name" }, "type='table'", null, null, null, null, "1");
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
        String query = "type='table' AND name <> 'android_metadata'";
        Cursor cursor = db.query("sqlite_master", null, query, null, null, null, null);
        ArrayList<String> tableList = new ArrayList<String>();
        while (cursor.moveToNext()) {
            tableList.add(SQLiteUtils.safeGetStringFromCursor(cursor, "name"));
        }
        cursor.close();
        return tableList;
    }

    /**
     * Retrieve a Long from a cursor, or null if a long could not be retrieved.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnIndex The index of the column from which to retrieve the value.
     * @return The Long value, or null.
     */
    public static Long safeGetLongFromCursor(Cursor cursor, int columnIndex) {
        try {
            return cursor.getLong(columnIndex);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Retrieve a Long from a cursor, or null if a long could not be retrieved.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnName The name of the column from which to retrieve the value.
     * @return The Long value, or null.
     */
    public static Long safeGetLongFromCursor(Cursor cursor, String columnName) {
        return safeGetLongFromCursor(cursor, cursor.getColumnIndex(columnName));
    }

    /**
     * Retrieve a long from a cursor, or a default value.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnIndex The index of the column from which to retrieve the value.
     * @param defaultValue The value to return if none was retrieved.
     * @return The long value, or the default.
     */
    public static long safeGetLongFromCursor(Cursor cursor, int columnIndex, long defaultValue) {
        try {
            return cursor.getLong(columnIndex);
        }
        catch (Exception e) {
            e.printStackTrace();
            return defaultValue;
        }
    }

    /**
     * Retrieve a long from a cursor, or a default value if a long could not be retrieved.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnName The name of the column from which to retrieve the value.
     * @param defaultValue The value to return if none was retrieved.
     * @return The long value, or the default.
     */
    public static long safeGetLongFromCursor(Cursor cursor, String columnName, long defaultValue) {
        return safeGetLongFromCursor(cursor, cursor.getColumnIndex(columnName), defaultValue);
    }

    /**
     * Retrieve an Integer from a cursor, or null if an int could not be retrieved.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnIndex The index of the column from which to retrieve the value.
     * @return The Integer value, or null.
     */
    public static Integer safeGetIntFromCursor(Cursor cursor, int columnIndex) {
        try {
            return cursor.getInt(columnIndex);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Retrieve an Integer from a cursor, or null if an int could not be retrieved.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnName The name of the column from which to retrieve the value.
     * @return The Integer value, or null.
     */
    public static Integer safeGetIntFromCursor(Cursor cursor, String columnName) {
        return safeGetIntFromCursor(cursor, cursor.getColumnIndex(columnName));
    }

    /**
     * Retrieve an int from a cursor, or a default value.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnIndex The index of the column from which to retrieve the value.
     * @param defaultValue The value to return if none was retrieved.
     * @return The int value, or the default.
     */
    public static int safeGetIntFromCursor(Cursor cursor, int columnIndex, int defaultValue) {
        try {
            return cursor.getInt(columnIndex);
        }
        catch (Exception e) {
            e.printStackTrace();
            return defaultValue;
        }
    }

    /**
     * Retrieve an int from a cursor, or a default value.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnName The name of the column from which to retrieve the value.
     * @param defaultValue The value to return if none was retrieved.
     * @return The int value, or the default.
     */
    public static int safeGetIntFromCursor(Cursor cursor, String columnName, int defaultValue) {
        return safeGetIntFromCursor(cursor, cursor.getColumnIndex(columnName), defaultValue);
    }

    /**
     * Retrieve a String from a cursor, or null.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnIndex The index of the column from which to retrieve the value.
     * @return The String value, or null.
     */
    public static String safeGetStringFromCursor(Cursor cursor, int columnIndex) {
        try {
            return cursor.getString(columnIndex);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Retrieve a String from a cursor, or null if a String could not be retrieved.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnName The name of the column from which to retrieve the value.
     * @return The String value, or null.
     */
    public static String safeGetStringFromCursor(Cursor cursor, String columnName) {
        return safeGetStringFromCursor(cursor, cursor.getColumnIndex(columnName));
    }

    /**
     * Retrieve a String from a cursor, or a default value.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnIndex The index of the column from which to retrieve the value.
     * @param defaultValue The value to return if none was retrieved.
     * @return The String value, or null.
     */
    public static String safeGetStringFromCursor(Cursor cursor, int columnIndex, String defaultValue) {
        try {
            return cursor.getString(columnIndex);
        }
        catch (Exception e) {
            e.printStackTrace();
            return defaultValue;
        }
    }

    /**
     * Retrieve a String from a cursor, or a default value.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnName The name of the column from which to retrieve the value.
     * @param defaultValue The value to return if none was retrieved.
     * @return The String value, or the default.
     */
    public static String safeGetStringFromCursor(Cursor cursor, String columnName, String defaultValue) {
        return safeGetStringFromCursor(cursor, cursor.getColumnIndex(columnName), defaultValue);
    }


    /**
     * Retrieve a byte array from a cursor, or null if a byte array could not be retrieved.
     *
     * @param cursor The cursor from which to retrieve the value. Must be at a valid position.
     * @param columnIndex The index of the column from which to retrieve the value.
     * @return The String value, or null.
     */
    public static byte[] safeGetByteArrayFromCursor(Cursor cursor, int columnIndex) {
        try {
            return cursor.getBlob(columnIndex);
        }
        catch (SQLiteException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            if (cursor != null) {
                cursor.close();
            }
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