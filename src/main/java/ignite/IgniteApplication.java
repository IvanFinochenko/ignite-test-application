package ignite;

import entity.CarWashUser;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import rdbms.jdbc.JDBConnection;
import system.Parameters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class IgniteApplication {
    private Parameters parameters;
    private JDBConnection jdbc;
    private Ignite ignite;

    private IgniteCache subscriberCache;
    private IgniteCache callCache;
    private IgniteCache carWashCache;
    private IgniteCache carWashUsersCache;

    public IgniteApplication(Ignite ignite, Parameters parameters) throws SQLException {
        this.parameters = parameters;
        jdbc = new JDBConnection();
        this.ignite = ignite;
    }

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

    public void start() throws SQLException {
        System.out.println();
        System.out.println(">>> Ignite application started.");

        setupCashes();
        sampleData();
        insertCarWashUser();
        getCarWashUsers();
        printResult();
    }

    void setupCashes() {
        // Getting a reference to Subscriber, Call, CarWash and CarWashUsers caches created by IgniteSourceService
        subscriberCache = ignite.cache("SQL_PUBLIC_SUBSCRIBER");
        callCache = ignite.cache("SQL_PUBLIC_CALL");
        carWashCache = ignite.cache("SQL_PUBLIC_CARWASH");
        carWashUsersCache = ignite.cache("SQL_PUBLIC_CARWASHUSER");
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
        List<CarWashUser> users = getCarWashUsers();

        SqlFieldsQuery queryInsert = new SqlFieldsQuery("INSERT INTO CARWASHUSER (" +
                "subs_key, cunc_ind, name) VALUES (?, ?, ?)");

        users.forEach((usr) -> {
                    System.out.println("Inserting: " + usr);
                    carWashUsersCache.query(queryInsert.setArgs(
                            usr.getSubsKey(), usr.getConcInd(), usr.getName())).getAll();
                }
        );

        System.out.println(">>> Inserted entries into CarWashUser:" + carWashUsersCache.size(CachePeekMode.PRIMARY));
    }

    private List<CarWashUser> getCarWashUsers() {
        List<CarWashUser> users = new LinkedList<>();
        SqlFieldsQuery query = new SqlFieldsQuery(
                "SELECT DISTINCT s.subs_key, cw.cunc_ind, cwFriend.name" +
                        " FROM SUBSCRIBER s JOIN CALL c ON s.subs_key = c.subs_from " +
                        " JOIN CARWASH cw ON cw.subs_key = c.subs_to " +
                        " LEFT JOIN (" +
                        "      SELECT place, name" +
                        "      FROM CARWASH " +
                        "      WHERE cunc_ind = 1 " +
                        "      LIMIT 1) cwFriend" +
                        "   ON cwFriend.place = cw.place " +
                        " WHERE c.dur >= 60 AND s.time_key < " +
                        "'" + parameters.today.minusYears(1) + "'");

        query.setDistributedJoins(true);

        carWashUsersCache.query(query).getAll().forEach( (usr) -> {
            ArrayList arr = (ArrayList) usr;
            users.add(new CarWashUser((long) arr.get(0), (int) arr.get(1), arr.get(2).toString()));
        });


        return users;
    }

    void printResult() {
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT * " +
                " FROM CARWASHUSER LIMIT 10");

        try (FieldsQueryCursor cursorSubscriber = carWashUsersCache.query(query)) {
            System.out.println("------------CARWASHUSERS:--------------");
            cursorSubscriber.forEach(System.out::println);
            System.out.println("---------------------------------------");
        }
    }


}

