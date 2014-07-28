package com.whitelightgrp.mobility.android.database;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * A minimal implementation of SQLiteOpenHelper that stores a single database reference for all callers to use.
 *
 * @author Justin Rohde, WhiteLight Group
 */
public class OpenHelper extends SQLiteOpenHelper {

    /**
     * Database name.
     */
    private final static String DATABASE_NAME = "RFgen";
    /**
     * Database version.
     */
    private final static int DATABASE_VERSION = 2;

    /**
     * The one and only instance of the database.
     */
    private static SQLiteDatabase db = null;

    /**
     * Return the single SQLiteDatabase instance, creating it beforehand if needed.
     *
     * @param context The {@link Context} used to open or create the database.
     * @return The database instance.
     */
    public static SQLiteDatabase getDatabase(Context context) {
        return db == null ? db = new OpenHelper(context).getWritableDatabase() : db;
    }

    /**
     * Constructor.
     *
     * @param context The {@link Context} used to open the database.
     */
    private OpenHelper(Context context) {
        // Pass a custom cursor factory so that query text may be logged
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
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
