package rdbms;

import utils.SqlScripts;

import java.sql.*;
import java.time.LocalDateTime;

public class InitializerH2 {
    private Connection conn;
    private Object[] fields;

    public InitializerH2(Connection conn) {
        this.conn = conn;
    }

    public void init() throws SQLException{
        conn.setAutoCommit(false);

        try (Statement stmt = conn.createStatement()){
            dropTables(stmt);
            createTables(stmt);
            stmt.executeBatch();
            conn.commit();
        }

        insertData();
        conn.commit();
    }

    private void dropTables(Statement stmt) {
        try {

            stmt.addBatch(SqlScripts.getSql("drop_subscriber"));
            stmt.addBatch(SqlScripts.getSql("drop_call"));
            stmt.addBatch(SqlScripts.getSql("drop_carwash"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createTables(Statement stmt) {
        try {

            stmt.addBatch(SqlScripts.getSql("create_subscriber"));
            stmt.addBatch(SqlScripts.getSql("create_call"));
            stmt.addBatch(SqlScripts.getSql("create_carwash"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertData() {
        SourceService sourceService = new SourceServiceExampleImpl();

        String sqlSubs = "INSERT INTO Subscriber VALUES (?, ?, ?, ?)";
        String sqlCall = "INSERT INTO Call VALUES (?, ?, ?, ?, ?)";
        String sqlCarWash = "INSERT INTO CarWash VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sqlSubs)) {
            sourceService.getSubscribers().forEach(subs -> {
                try {
                    fields = new Object[] {
                            subs.getSubsKey(),
                            subs.getName(),
                            subs.getPlace(),
                            Date.valueOf(subs.getTimeKey())
                    };
                    addPreparedBatch(pstmt, fields);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            pstmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (PreparedStatement pstmt = conn.prepareStatement(sqlCall)) {
            sourceService.getCalls(LocalDateTime.now().minusYears(2), LocalDateTime.now()).forEach(call -> {
                try {
                    fields = new Object[] {
                            call.id,
                            call.getSubsFrom(),
                            call.getSubsTo(),
                            call.getDur(),
                            call.getStartTime().toString()
                    };
                    addPreparedBatch(pstmt, fields);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            pstmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (PreparedStatement pstmt = conn.prepareStatement(sqlCarWash)) {
            sourceService.getCarWashes().forEach((carWash -> {
                try {
                    fields = new Object[] {
                            carWash.getSubsKey(),
                            carWash.getName(),
                            carWash.getPlace(),
                            carWash.getConcInd()
                    };
                    addPreparedBatch(pstmt, fields);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }));
            pstmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void addPreparedBatch(PreparedStatement pstmt, Object[] fields) throws SQLException {
        try {
            for (int i = 1; i <= fields.length; ++i) {
                pstmt.setObject(i, fields[i - 1]);
            }
            pstmt.addBatch();
        } catch (SQLException e) {
            throw new SQLException(e);
        }
    }
}
