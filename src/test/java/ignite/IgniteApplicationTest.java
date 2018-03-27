package ignite;

import entity.Call;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.junit.Before;
import org.junit.Test;
import rdbms.SourceService;
import rdbms.SourceServiceExampleImpl;
import rdbms.jdbc.JDBConnection;
import system.Parameters;

import java.sql.SQLException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IgniteApplicationTest {
    Parameters parameters;
    SourceService sourceService;
    IgniteApplication igniteApplication;

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        String[] args = new String[1];
        args[0] = "2018-03-25";

        parameters = Parameters.getInstance(args);
        sourceService = new SourceServiceExampleImpl();
        igniteApplication = new IgniteApplication(parameters, sourceService);
    }

    @Test
    public void testApp() throws SQLException {
        JDBConnection jdbConnection = new JDBConnection();

        try (Ignite ignite = Ignition.start()) {
            jdbConnection.createTablesWithIndexes();

            igniteApplication.setupCashes(ignite);
            IgniteCache subscriberCache = igniteApplication.getSubscriberCache();
            IgniteCache callCache = igniteApplication.getCallCache();
            IgniteCache carWashCache = igniteApplication.getCarWashCache();
            IgniteCache carWashUsersCache = igniteApplication.getCarWashUsersCache();

            igniteApplication.insertData();

            assertFalse("Subscriber table is empty",
                    subscriberCache.size(CachePeekMode.ALL) == 0);

            assertFalse("Call table is empty",
                    callCache.size(CachePeekMode.ALL) == 0);

            assertFalse("CarWash table is empty",
                    carWashCache.size(CachePeekMode.ALL) == 0);

            assertTrue("CarWashUser table isn't empty",
                    carWashUsersCache.size(CachePeekMode.ALL) == 0);

            igniteApplication.insertCarWashUser();
            int cntUsersBefore = carWashUsersCache.size(CachePeekMode.ALL);
            assertFalse("CarWashUser table is empty", cntUsersBefore == 0);

            int cntCallsBefore = callCache.size(CachePeekMode.ALL);
            addValidValueToCallsTable(callCache);
            int cntCallsAfter = callCache.size(CachePeekMode.ALL);
            assertTrue("Number of Calls should be increased by 1",
                    cntCallsBefore + 1 == cntCallsAfter);

            carWashUsersCache.clear();
            igniteApplication.insertCarWashUser();
            int cntUsersAfter = carWashUsersCache.size(CachePeekMode.ALL);
            assertTrue("Number of Carwash users should be increased by 1 after adding valid call",
                    cntUsersBefore + 1 == cntUsersAfter);

            carWashUsersCache.clear();
            igniteApplication.insertCarWashUser();
            int cntUsersAfterWrondCalls = carWashUsersCache.size(CachePeekMode.ALL);
            assertTrue("Number of Carwash users should be equal value before, " +
                            "because wrong values were added into Calls ",
                    cntUsersAfter == cntUsersAfterWrondCalls);
        }
    }

    private void addValidValueToCallsTable(IgniteCache callCache) {
        SqlFieldsQuery queryCall = new SqlFieldsQuery("INSERT INTO Call (" +
                "id, subs_from, subs_to, dur, start_time) VALUES (?, ?, ?, ?, ?)");

            //valid call
            callCache.query(queryCall.setArgs(
                    Call.INSTANCE_COUNT++,
                    89202550011L,
                    88002553534L,
                    61,
                    parameters.today.minusDays(1))).getAll();

    }

    private void addNotValidValuesToCallsTable(IgniteCache callCache) {
        SqlFieldsQuery queryCall = new SqlFieldsQuery("INSERT INTO Call (" +
                "id, subs_from, subs_to, dur, start_time) VALUES (?, ?, ?, ?, ?)");

        //dur < 60
        callCache.query(queryCall.setArgs(
                Call.INSTANCE_COUNT++,
                89202550011L,
                88002553534L,
                59,
                parameters.today.minusDays(1))).getAll();

        //subs_to is wrong
        callCache.query(queryCall.setArgs(
                Call.INSTANCE_COUNT++,
                89202550011L,
                80000000000L,
                70,
                parameters.today.minusDays(1))).getAll();

        //future time
        callCache.query(queryCall.setArgs(
                Call.INSTANCE_COUNT++,
                89202550011L,
                80000000000L,
                59,
                parameters.today.plusDays(1))).getAll();

    }
}
