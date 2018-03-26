package rdbms;

import entity.Call;
import entity.CarWash;
import entity.Subscriber;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class SourceServiceH2Impl implements SourceService {
    private Connection conn;

    public SourceServiceH2Impl() throws ClassNotFoundException, SQLException {
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:~/test", "sa", "");

        dropTables();
        createTables();
        insertData();
    }

    private void insertData() {
        SourceService sourceService = new SourceServiceExampleImpl();

        sourceService.getSubscribers().forEach(subs -> {
            try {
                insertSubscriber(subs);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        sourceService.getCalls(LocalDateTime.now().minusYears(2), LocalDateTime.now()).forEach(call -> {
            try {
                insertCall(call);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        sourceService.getCarWashes().forEach((carWash -> {
            try {
                insertCarWash(carWash);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }));
    }

    private void createTables() {
        try (Statement stmt = conn.createStatement()) {

            String sqlSubscriber = "CREATE TABLE IF NOT EXISTS Subscriber (" +
                    " subs_key LONG PRIMARY KEY," +
                    " place VARCHAR," +
                    " name VARCHAR," +
                    " time_key DATE) ";
            stmt.executeUpdate(sqlSubscriber);

            String sqlCall = "CREATE TABLE IF NOT EXISTS Call (" +
                    " id INT PRIMARY KEY," +
                    " subs_from LONG," +
                    " subs_to LONG," +
                    " dur INT," +
                    " start_time VARCHAR) ";
            stmt.executeUpdate(sqlCall);

            String sqlCarWash = "CREATE TABLE IF NOT EXISTS CarWash (" +
                    " subs_key LONG PRIMARY KEY," +
                    " name VARCHAR," +
                    " place VARCHAR," +
                    " cunc_ind INT) ";
            stmt.executeUpdate(sqlCarWash);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertSubscriber(Subscriber subs) throws SQLException {
        String sql = "INSERT INTO Subscriber VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, subs.getSubsKey());
            pstmt.setString(2, subs.getName());
            pstmt.setString(3, subs.getPlace());
            pstmt.setDate(4, Date.valueOf(subs.getTimeKey()));
            pstmt.executeUpdate();
        } catch (SQLException e) {
            if (e.getMessage().contains("Unique index or primary key violation")) {
                System.out.println(">>> Insert into Subscriber was rejected " +
                        "because it has already contained such primary key");
            } else {
                throw new SQLException(e);
            }
        }
    }

    private void insertCall(Call call) throws SQLException {
        String sql = "INSERT INTO Call VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, Call.INSTANCE_COUNT++);
            pstmt.setLong(2, call.getSubsFrom());
            pstmt.setLong(3, call.getSubsTo());
            pstmt.setLong(4, call.getDur());
            pstmt.setString(5, call.getStartTime().toString());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            if (e.getMessage().contains("Unique index or primary key violation")) {
                System.out.println(">>> Insert into Call was rejected " +
                        "because it has already contained such primary key");
            } else {
                throw new SQLException(e);
            }
        }
    }

    private void insertCarWash(CarWash cw) throws SQLException {
        String sql = "INSERT INTO CarWash VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, cw.getSubsKey());
            pstmt.setString(2, cw.getName());
            pstmt.setString(3, cw.getPlace());
            pstmt.setInt(4, cw.getConcInd());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            if (e.getMessage().contains("Unique index or primary key violation")) {
                System.out.println(">>> Insert into Subscriber was rejected " +
                        "because it has already contained such primary key");
            } else {
                throw new SQLException(e);
            }
        }
    }

    private void dropTables() {
        try (Statement stmt = conn.createStatement()) {

            String sqlSubscriber = "DROP TABLE IF EXISTS Subscriber";
            stmt.executeUpdate(sqlSubscriber);

            String sqlCall = "DROP TABLE IF EXISTS Call ";
            stmt.executeUpdate(sqlCall);

            String sqlCarWash = "DROP TABLE IF EXISTS CarWash";
            stmt.executeUpdate(sqlCarWash);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public List<Call> getCalls(LocalDateTime dateFrom, LocalDateTime dateUpTo) {
        List<Call> calls = new LinkedList<>();

        try (Statement stmt = conn.createStatement()) {
            String sql = "SELECT subs_from, subs_to, dur, start_time " +
                    "FROM Call " +
                    "WHERE start_time > \'" + dateFrom + "\'"+
                    "  AND start_time < \'" + dateUpTo + "\'";

            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                // Retrieve by column name
                long subsFrom = rs.getLong("subs_from");
                long subsTo = rs.getLong("subs_to");
                int dur = rs.getInt("dur");
                LocalDateTime startTime = LocalDateTime.parse(rs.getString("start_time"));

                calls.add(new Call(subsFrom, subsTo, dur, startTime));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return calls;
    }

    @Override
    public List<Subscriber> getSubscribers() {
        List<Subscriber> subs = new LinkedList<>();

        try (Statement stmt = conn.createStatement()) {
            String sql = "SELECT subs_key, place, name, time_key FROM Subscriber";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                // Retrieve by column name
                long subsKey = rs.getLong("subs_key");
                String place = rs.getString("place");
                String name = rs.getString("name");
                LocalDate timeKey = LocalDate.parse(rs.getDate("time_key").toString());

                subs.add(new Subscriber(subsKey, place, name, timeKey));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return subs;
    }

    @Override
    public List<CarWash> getCarWashes() {
        List<CarWash> washes = new LinkedList<>();

        try (Statement stmt = conn.createStatement()) {
            String sql = "SELECT subs_key, place, name, cunc_ind FROM CarWash";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                // Retrieve by column name
                long subsKey = rs.getLong("subs_key");
                String name = rs.getString("name");
                String place = rs.getString("place");
                int concInd = rs.getInt("cunc_ind");

                washes.add(new CarWash(subsKey, name, place, concInd));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return washes;
    }
}
