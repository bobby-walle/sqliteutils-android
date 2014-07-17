package com.whitelightgrp.mobility.android.icrptesting.sqliteopenhelperimpl;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * A basic implementation of SQLiteOpenHelper, intended to simplify database access.
 *
 * @author Justin Rohde
 */
public class SQLiteOpenHelperImpl extends SQLiteOpenHelper {
    /**
     * The database name.
     */
    private static final String DATABASE_NAME = "";
    /**
     * The database version number.
     */
    private static final int DATABASE_VERSION = 1;
    /**
     * The single instance of the database.
     */
    private static SQLiteDatabase db;

    /**
     * Constructor.
     *
     * @param context The context used to create or open the database.
     */
    public SQLiteOpenHelperImpl(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    /**
     * Return the current database instance, initializing it first if needed.
     *
     * @param context The context used to create or open the database.
     * @return The database instance.
     */
    public static SQLiteDatabase getDatabase(Context context) {
        if (db == null) {
            db = new SQLiteOpenHelperImpl(context).getWritableDatabase();
        }
        return db;
    }

    /**
     * Clear the current database reference so the next call to {@link #getDatabase(android.content.Context)} will retrieve the newest database.
     */
    public static void reset() {
        db = null;
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        // Not implemented
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        // Not implemented
    }
}
