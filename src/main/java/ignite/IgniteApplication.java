package ignite;

import entity.Call;
import entity.CarWashUser;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import rdbms.SourceService;
import rdbms.jdbc.JDBConnection;
import system.Parameters;

import java.sql.SQLException;
import java.util.List;

public class IgniteApplication {
    private Parameters parameters;
    private SourceService sourceService;
    private JDBConnection jdbc;

    private IgniteCache subscriberCache;
    private IgniteCache callCache;
    private IgniteCache carWashCache;
    private IgniteCache carWashUsersCache;

    IgniteCache getSubscriberCache() {
        return subscriberCache;
    }

    IgniteCache getCallCache() {
        return callCache;
    }

    IgniteCache getCarWashCache() {
        return carWashCache;
    }

    IgniteCache getCarWashUsersCache() {
        return carWashUsersCache;
    }

    public IgniteApplication(Parameters parameters, SourceService sourceService) throws SQLException {
        this.parameters = parameters;
        this.sourceService = sourceService;
        jdbc = new JDBConnection();
    }

    public void start() throws SQLException {
        //Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            jdbc.createTablesWithIndexes();

            System.out.println();
            System.out.println(">>> Ignite application started.");

            setupCashes(ignite);

            insertData();

            sampleData();

            insertCarWashUser();

            printResult();
        }
    }

    void setupCashes(Ignite ignite) {
        // Getting a reference to Subscriber, Call, CarWash and CarWashUsers caches created by JBCConnection
        subscriberCache = ignite.cache("SQL_PUBLIC_SUBSCRIBER");
        callCache = ignite.cache("SQL_PUBLIC_CALL");
        carWashCache = ignite.cache("SQL_PUBLIC_CARWASH");
        carWashUsersCache = ignite.cache("SQL_PUBLIC_CARWASHUSER");
    }

    void insertData() {
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
                "subs_key, name, place, cunc_ind) VALUES (?, ?, ?, ?)");

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

    void sampleData() {
        SqlFieldsQuery querySubscriber = new SqlFieldsQuery("SELECT * " +
                " FROM SUBSCRIBER LIMIT 10");

        FieldsQueryCursor cursorSubscriber = subscriberCache.query(querySubscriber);

        System.out.println("SUBSCRIBER:");
        cursorSubscriber.forEach(System.out::println);

        SqlFieldsQuery queryCall = new SqlFieldsQuery("SELECT * " +
                " FROM CALL LIMIT 10");

        FieldsQueryCursor cursorCall = callCache.query(queryCall);

        System.out.println("CALL:");
        cursorCall.forEach(System.out::println);

        SqlFieldsQuery queryCarWash = new SqlFieldsQuery("SELECT * " +
                " FROM CARWASH LIMIT 10");

        FieldsQueryCursor cursorCarWash = carWashCache.query(queryCarWash);

        System.out.println("CARWASH:");
        cursorCarWash.forEach(System.out::println);
    }

    /**
     * Main method for calculate CarWashUsers
     */
    void insertCarWashUser() throws SQLException {
        List<CarWashUser> users = jdbc.getCarWashUsers(parameters);

        SqlFieldsQuery queryInsert = new SqlFieldsQuery("INSERT INTO CARWASHUSER (" +
                "subs_key, cunc_ind, name) VALUES (?, ?, ?)");

        users.forEach((usr) -> {
                        System.out.println("Inserting: " + usr);
                carWashUsersCache.query(queryInsert.setArgs(
                        usr.getSubsKey(), usr.getConcInd(), usr.getName())).getAll(); }
        );

        System.out.println(">>> Inserted entries into CarWashUser:" + carWashUsersCache.size(CachePeekMode.PRIMARY));
    }

    void printResult() {
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT * " +
                " FROM CARWASHUSER LIMIT 10");

        FieldsQueryCursor cursorSubscriber = carWashUsersCache.query(query);

        System.out.println("------------CARWASHUSERS:--------------");
        cursorSubscriber.forEach(System.out::println);
        System.out.println("---------------------------------------");
    }


}

