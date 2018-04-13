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
import utils.SqlScripts;

import javax.cache.Cache;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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

    IgniteCache<Long, Subscriber> getSubscriberCache() {
        return subscriberCache;
    }

    IgniteCache<Long, Call> getCallCache() {
        return callCache;
    }

    IgniteCache<Long, CarWash> getCarWashCache() {
        return carWashCache;
    }

    IgniteCache<Long, CarWashUser> getCarWashUsersCache() {
        return carWashUsersCache;
    }

    public void start() throws SQLException {
        System.out.println();
        System.out.println(">>> Ignite application started.");

        setupCashes();
        sampleData();
//        insertCarWashUser();
        computeCarWashUsers();
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
        SqlFieldsQuery query = new SqlFieldsQuery(SqlScripts.getSql("carwash_user"))
                .setArgs(parameters.today.minusYears(1));

        query.setDistributedJoins(true);

        carWashUsersCache.query(query).getAll().forEach((usr) -> {
            ArrayList arr = (ArrayList) usr;
            users.add(new CarWashUser((long) arr.get(0), (int) arr.get(1), arr.get(2).toString()));
        });


        return users;
    }

    public void computeCarWashUsers() {
        carWashUsersCache.clear();
        ignite.compute().run(() -> {
            CarWash cwFriend = carWashCache.query(new SqlQuery<Long, CarWash>(CarWash.class, "cuncInd = 1 LIMIT 1"))
                .getAll().get(0).getValue();

            AtomicReference<Subscriber> sub = new AtomicReference<>();
            AtomicReference<CarWash> carWash = new AtomicReference<>();

            callCache.query(new ScanQuery<Long, Call>()).forEach(callEntry -> {
                if (subscriberCache.containsKey(callEntry.getValue().getSubsFrom())
                        && carWashCache.containsKey(callEntry.getValue().getSubsTo())) {
                    sub.set(subscriberCache.get(callEntry.getValue().getSubsFrom()));
                    carWash.set(carWashCache.get(callEntry.getValue().getSubsTo()));
                    if (carWash.get().getPlace().equals(cwFriend.getPlace())
                            && callEntry.getValue().getDur() >= 60
                            && sub.get().getTimeKey().compareTo(parameters.today.minusYears(1)) < 0) {
                        carWashUsersCache.put(
                                sub.get().getSubsKey(),
                                new CarWashUser(sub.get().getSubsKey(), carWash.get().getConcInd(), cwFriend.getName()));
                    }
                }
            });
        });
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

