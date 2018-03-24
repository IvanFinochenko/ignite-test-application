package examples;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConnectionManegerTest {
    @Test
    public void connectionSingletonPatternWorks() throws ClassNotFoundException {
        ConnectionManager connectionManager = ConnectionManager.getInstance();
        assertNotNull("connection should be initialized", connectionManager);

        ConnectionManager secondConnectionManager = ConnectionManager.getInstance();
        assertTrue("connection has exactly single instance",
                connectionManager == secondConnectionManager);
    }

    @Test
    public void tablesTest() throws ClassNotFoundException, SQLException {
        ConnectionManager connectionManager = ConnectionManager.getInstance();

        //connectionManager.createTables();
        //connectionManager.insertData();
        connectionManager.execSampleQuery();
    }
}

