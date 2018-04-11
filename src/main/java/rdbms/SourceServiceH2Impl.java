package rdbms;

import entity.Call;
import entity.CarWash;
import entity.Subscriber;
import org.apache.ignite.internal.sql.SqlParseException;
import utils.SqlScripts;

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

        InitializerH2 initializerH2 = new InitializerH2(conn);
        initializerH2.init();
    }
    
    @Override
    public List<Call> getCalls(LocalDateTime dateFrom, LocalDateTime dateUpTo) {
        List<Call> calls = new LinkedList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(SqlScripts.getSql("select_call"))) {
            pstmt.setString(1, dateFrom.toString());
            pstmt.setString(2, dateUpTo.toString());
            ResultSet rs = pstmt.executeQuery();

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
            ResultSet rs = stmt.executeQuery(SqlScripts.getSql("select_subscriber"));

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
            ResultSet rs = stmt.executeQuery(SqlScripts.getSql("select_carwash"));

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
