package com.april1985;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: derek
 * Date: 13-4-17
 * Time: 下午10:19
 * To change this template use File | Settings | File Templates.
 */
public class CsvImport implements Runnable {
    public CsvImport(File csvFile, DB ticketsDB) {
        this.csvFile = csvFile;
        this.ticketDB = ticketsDB;
    }

    private File csvFile;
    private DB ticketDB;

    @Override
    public void run() {
        DateFormat fetchTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        fetchTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT+0"));

        DateFormat tf = new SimpleDateFormat("HH:mm:ss");
        tf.setTimeZone(TimeZone.getTimeZone("GMT+0"));

        try {
            long startTime = System.currentTimeMillis();

            System.out.println(csvFile);
            BufferedReader reader = new BufferedReader(new FileReader(csvFile));

            int totalRecords = 0;
            int batch = 5000;

            List<DBObject> objectList = new ArrayList<DBObject>(batch);

            DBCollection collection = null;

            while (reader.ready()) {
                String line = reader.readLine();
                String[] lines = line.split(",");
                GregorianCalendar fetchTimeCal = new GregorianCalendar();
                Date fetchTime = fetchTimeFormat.parse(lines[1]);
                fetchTimeCal.setTime(fetchTime);

                String airline = lines[2];
                int price = Integer.parseInt(lines[3]);
                if (price == 0) continue;
                int discount = (int) (Float.parseFloat(lines[4]) * 10);
                Date departDate = fetchTimeFormat.parse(lines[5]);
                Date departTime = tf.parse(lines[6]);
                Date arriveTime = tf.parse(lines[7]);
                String source = lines[10].replace("QUNAR", "Q").replace("KUXUN", "K").replace("TAOBAO", "T");

                String collectionName = String.format("db_%d_%02d_%02d", fetchTimeCal.get(Calendar.YEAR), fetchTimeCal.get(Calendar.MONTH) + 1, fetchTimeCal.get(Calendar.DAY_OF_MONTH));

                collection = ticketDB.getCollection(collectionName);
                BasicDBObject dbObject = new BasicDBObject("ft", fetchTime)
                        .append("al", airline)
                        .append("pr", price)
                        .append("di", discount)
                        .append("dd", departDate)
                        .append("dt", departTime)
                        .append("at", arriveTime)
                        .append("sr", source);

                objectList.add(dbObject);

                if ((totalRecords++) % batch == 0) {
                    System.out.printf("%s: %d\n", csvFile.getName(), totalRecords);
                    collection.insert(objectList);
                    objectList.clear();
                }
            }

            if (collection != null) collection.insert(objectList);

            long endTime = System.currentTimeMillis();

            System.out.println("[FINISHED]" + csvFile.getName() + ":" + totalRecords + ":" + ((endTime - startTime) / 1000) + "  seconds");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
