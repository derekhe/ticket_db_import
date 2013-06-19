package com.april1985;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.File;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


/**
 * Created with IntelliJ IDEA.
 * User: derek
 * Date: 13-4-18
 * Time: 下午9:33
 * To change this template use File | Settings | File Templates.
 */
public class Sqlite3Import implements Runnable {
    private File sqlite3DB;
    private DB ticketDB;

    public Sqlite3Import(File sqlite3DB, DB ticketsDB) {
        this.sqlite3DB = sqlite3DB;
        this.ticketDB = ticketsDB;
    }

    @Override
    public void run() {
        try {
            System.out.println(sqlite3DB);
            long startTime = System.currentTimeMillis();

            Class.forName("org.sqlite.JDBC");

            Connection connection = null;

            String dbName = "jdbc:sqlite:" + sqlite3DB.getAbsolutePath();
            connection = DriverManager.getConnection(dbName);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            ResultSet rs = statement.executeQuery("select * from ticket");

            DateFormat fetchTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
            fetchTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT+0"));

            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+0"));

            DateFormat departTimeFormat = new SimpleDateFormat("HH:mm:ss");
            departTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT+0"));

            int totalRecords = 0;
            int batch = 5000;

            List<DBObject> objectList = new ArrayList<DBObject>(batch);

            DBCollection collection = null;

            while (rs.next()) {
                Date fetchTime = fetchTimeFormat.parse(rs.getString("fetchTime"));
                String airline = rs.getString("airline");
                int price = rs.getInt("price");
                if (price == 0) continue;
                int discount = rs.getInt("discount");
                Date departDate = dateFormat.parse(rs.getString("date"));
                Date departTime = departTimeFormat.parse(rs.getString("departureTime") + ":00");
                Date arriveTime = departTimeFormat.parse(rs.getString("arriveTime") + ":00");
                String source = rs.getString("priceSource");

                GregorianCalendar fetchTimeCal = new GregorianCalendar();
                fetchTimeCal.setTime(fetchTime);


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
                    System.out.printf("%s: %d\n", sqlite3DB.getName(), totalRecords);
                    collection.insert(objectList);
                    objectList.clear();
                }
            }

            if (collection != null) collection.insert(objectList);

            long endTime = System.currentTimeMillis();

            System.out.println("[FINISHED]" + sqlite3DB.getName() + ":" + totalRecords + ":" + ((endTime - startTime) / 1000) + "  seconds");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}