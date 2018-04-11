package ignite;

import entity.Call;
import entity.CarWash;
import entity.CarWashUser;
import entity.Subscriber;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.*;
import system.Parameters;

import javax.cache.Cache;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IgniteApplication {
    private Parameters parameters;
    private Ignite ignite;

    private IgniteCache<Long, Subscriber> subscriberCache;
    private IgniteCache<Long, Call> callCache;
    private IgniteCache<Long, CarWash> carWashCache;
    private IgniteCache<Long, CarWashUser> carWashUsersCache;

    public IgniteApplication(Ignite ignite, Parameters parameters) throws SQLException {
        this.parameters = parameters;
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
        subscriberCache = ignite.cache("SUBSCRIBER");
        callCache = ignite.cache("CALL");
        carWashCache = ignite.cache("CARWASH");
        carWashUsersCache = ignite.cache("CARWASHUSER");
    }

    private <V> void printCache(IgniteCache<Long, V> cache) {
        List<Cache.Entry<Long, V>> entries = cache.query(new ScanQuery<Long, V>()).getAll();
        entries.forEach(System.out::println);
    }

    private void sampleData() {
        System.out.println(">>> SUBSCRIBERS:");
        printCache(subscriberCache);
        System.out.println(">>> CALLS:");
        printCache(callCache);
        System.out.println(">>> CarWashes:");
        printCache(carWashCache);
    }

    /**
     * Main method for calculate CarWashUsers
     */
    void insertCarWashUser() throws SQLException {
        List<CarWashUser> users = getCarWashUsers();

        users.forEach((s) -> carWashUsersCache.put(s.getSubsKey(), s));

        System.out.println(">>> Inserted entries into CarWashUser:" + carWashUsersCache.size(CachePeekMode.PRIMARY));
    }

    private List<CarWashUser> getCarWashUsers() {
        List<CarWashUser> users = new LinkedList<>();
        SqlFieldsQuery query = new SqlFieldsQuery(
                "SELECT DISTINCT s.subsKey, cw.cuncInd, cwFriend.name" +
                        " FROM SUBSCRIBER s JOIN CALL c ON s.subsKey = c.subsFrom " +
                        " JOIN CARWASH cw ON cw.subsKey = c.subsTo " +
                        " LEFT JOIN (" +
                        "      SELECT place, name" +
                        "      FROM CARWASH " +
                        "      WHERE cuncInd = 1 " +
                        "      LIMIT 1) cwFriend" +
                        "   ON cwFriend.place = cw.place " +
                        " WHERE c.dur >= 60 AND s.timeKey < " +
                        "'" + parameters.today.minusYears(1) + "'");

        query.setDistributedJoins(true);

        carWashUsersCache.query(query).getAll().forEach((usr) -> {
            ArrayList arr = (ArrayList) usr;
            users.add(new CarWashUser((long) arr.get(0), (int) arr.get(1), arr.get(2).toString()));
        });


        return users;
    }

    private void printResult() {
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT * " +
                " FROM CARWASHUSER LIMIT 10");

        try (FieldsQueryCursor<List<?>> cursorSubscriber = carWashUsersCache.query(query)) {
            System.out.println("------------CARWASHUSERS:--------------");
            cursorSubscriber.forEach(System.out::println);
            System.out.println("---------------------------------------");
        }
    }


}

