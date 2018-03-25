package rdbms.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBConnection {
    public static void createTablesWithIndexes() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");

        try (Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Subscriber (" +
                    " subs_key LONG PRIMARY KEY," +
                    " place VARCHAR," +
                    " name VARCHAR," +
                    " time_key DATE) " +
                    " WITH \"backups=1\"");

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Call (" +
                    " id INT PRIMARY KEY," +
                    " subs_from LONG," +
                    " subs_to LONG," +
                    " dur INT," +
                    " start_time DATE) " +
                    " WITH \"backups=1\"");

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS CarWash (" +
                    " subs_key LONG PRIMARY KEY," +
                    " name VARCHAR," +
                    " place VARCHAR," +
                    " conc_ind INT) " +
                    " WITH \"backups=1\"");

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS CarWashUser (" +
                    " subs_key LONG PRIMARY KEY," +
                    " conc_ind INT," +
                    " name VARCHAR)" +
                    " WITH \"backups=1\"");

            stmt.executeUpdate("CREATE INDEX idx_subscriber_time_key ON Subscriber (time_key)");
            stmt.executeUpdate("CREATE INDEX idx_call_subscribers ON Call (subs_from, subs_to)");
            stmt.executeUpdate("CREATE INDEX idx_call_dur ON Call (dur)");
        }
    }
}
