package ignite;

import entity.Call;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import rdbms.SourceService;
import rdbms.jdbc.JDBConnection;
import system.Parameters;

import java.sql.SQLException;

public class IgniteApplication {
    private Parameters parameters;
    private SourceService sourceService;

    public IgniteApplication(Parameters parameters, SourceService sourceService) throws SQLException {
        this.parameters = parameters;
        this.sourceService = sourceService;
    }

    public void start() throws SQLException {
        //Ignition.start("examples/config/example-ignite.xml");

        //Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            JDBConnection.createTablesWithIndexes();

            System.out.println();
            System.out.println(">>> Ignite application started.");

            // Getting a reference to Subscriber, Call, CarWash and CarWashUsers caches created by JBCConnection
            IgniteCache subscriberCache = ignite.cache("SQL_PUBLIC_SUBSCRIBER");
            IgniteCache callCache = ignite.cache("SQL_PUBLIC_CALL");
            IgniteCache carWashCache = ignite.cache("SQL_PUBLIC_CARWASH");
            IgniteCache carWashUsersCache = ignite.cache("SQL_PUBLIC_CARWASHUSERS");


            insertData(subscriberCache, callCache, carWashCache);
        }
    }

    public void insertData(IgniteCache subscriberCache, IgniteCache callCache, IgniteCache carWashCache) {
        //--------------------INSERT TO SUBSCRIBER--------------------
        subscriberCache.clear();

        SqlFieldsQuery querySubs = new SqlFieldsQuery("INSERT INTO Subscriber (" +
                "subs_key, place, name, time_key) VALUES (?, ?, ?, ?)");

        sourceService.getSubscribers().forEach((s) ->
                subscriberCache.query(querySubs.setArgs(
                        s.getSubsKey(),
                        s.getPlace(),
                        s.getName(),
                        s.getTimeKey())).getAll()
        );

        System.out.println(">>> Inserted entries into Subscriber:" + subscriberCache.size(CachePeekMode.PRIMARY));

        //--------------------INSERT TO CALL--------------------
        callCache.clear();

        SqlFieldsQuery queryCall = new SqlFieldsQuery("INSERT INTO Call (" +
                "id, subs_from, subs_to, dur, start_time) VALUES (?, ?, ?, ?, ?)");

        sourceService.getCalls(parameters.today.atStartOfDay().minusWeeks(2), parameters.today.atStartOfDay())
                .forEach((s) ->
                        callCache.query(queryCall.setArgs(
                                Call.INSTANCE_COUNT++,
                                s.getSubsFrom(),
                                s.getSubsTo(),
                                s.getDur(),
                                s.getStartTime())).getAll()
                );

        System.out.println(">>> Inserted entries into Call:" + callCache.size(CachePeekMode.PRIMARY));

        //--------------------INSERT TO CARWASH--------------------
        carWashCache.clear();

        SqlFieldsQuery queryWash = new SqlFieldsQuery("INSERT INTO Carwash (" +
                "subs_key, name, place, conc_ind) VALUES (?, ?, ?, ?)");

        sourceService.getCarWashes()
                .forEach((s) ->
                        carWashCache.query(queryWash.setArgs(
                                s.getSubsKey(),
                                s.getName(),
                                s.getPlace(),
                                s.getConcInd())).getAll()
                );

        System.out.println(">>> Inserted entries into CarWash:" + carWashCache.size(CachePeekMode.PRIMARY));

    }
}

