package com.april1985;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import java.io.File;
import java.io.FilenameFilter;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        File dir = new File("/home/derek/dev/projects/raw_db/");
        File[] flightCSVs = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("flight") && name.contains(".csv");
            }
        });

        File[] flightSql3 = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(".db");
            }
        });

        MongoClient mongoClient;
        DB ticketsDB = null;
        try {
            mongoClient = new MongoClient();
            ticketsDB = mongoClient.getDB("tickets");
            ticketsDB.dropDatabase();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(32, 64, 10,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(64),
                new ThreadPoolExecutor.CallerRunsPolicy());

        for (File csv : flightCSVs) {
            threadPool.execute(new CsvImport(csv, ticketsDB));
        }

        for (File sql3 : flightSql3) {
            threadPool.execute(new Sqlite3Import(sql3, ticketsDB));
        }

        threadPool.shutdown();

        long endTime = System.currentTimeMillis();

        System.out.println("[FINISHED]" + ((endTime - startTime) / 1000) + "  seconds");
    }
}
