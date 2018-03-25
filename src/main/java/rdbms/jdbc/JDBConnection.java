package rdbms.jdbc;

import entity.CarWashUser;
import system.Parameters;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class JDBConnection {
    public void createTablesWithIndexes() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");

        try (Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Subscriber (" +
                    " subs_key LONG PRIMARY KEY," +
                    " place VARCHAR," +
                    " name VARCHAR," +
                    " time_key DATE) " +
                    " WITH \"template=replicated, backups=1\"");

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Call (" +
                    " id INT PRIMARY KEY," +
                    " subs_from LONG," +
                    " subs_to LONG," +
                    " dur INT," +
                    " start_time DATE) " +
                    " WITH \"template=partitioned,backups=1\"");

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS CarWash (" +
                    " subs_key LONG PRIMARY KEY," +
                    " name VARCHAR," +
                    " place VARCHAR," +
                    " cunc_ind INT) " +
                    " WITH \"template=replicated, backups=1\"");

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS CarWashUser (" +
                    " subs_key LONG PRIMARY KEY," +
                    " cunc_ind INT," +
                    " name VARCHAR)" +
                    " WITH \"template=replicated, backups=1\"");

            stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_subscriber_time_key ON Subscriber (time_key)");
            stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_call_subscribers ON Call (subs_from, subs_to)");
            stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_call_dur ON Call (dur)");
        }
    }

    public List<CarWashUser> getCarWashUsers(Parameters parameters) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
        List<CarWashUser> list = new LinkedList<>();

        try (Statement stmt = conn.createStatement()) {

            try (ResultSet rs =
                         stmt.executeQuery("SELECT DISTINCT s.subs_key, cw.cunc_ind, cwFriend.name" +
                                 " FROM SUBSCRIBER s JOIN CALL c ON s.subs_key = c.subs_from " +
                                 " JOIN CARWASH cw ON cw.subs_key = c.subs_to " +
                                 " LEFT JOIN (" +
                                 "      SELECT place, name" +
                                 "      FROM CARWASH " +
                                 "      WHERE cunc_ind = 1 " +
                                 "      LIMIT 1) cwFriend" +
                                 "   ON cwFriend.place = cw.place " +
                                 " WHERE c.dur >= 60 AND s.time_key < " +
                                 "'" + parameters.today.minusYears(1) + "'")) {

                while (rs.next())
                    list.add(new CarWashUser(rs.getLong(1), rs.getInt(2), rs.getString(3)));
            }
        }

        return list;

    }
}
