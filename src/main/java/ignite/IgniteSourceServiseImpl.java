package ignite;

import entity.Call;
import entity.CarWash;
import entity.CarWashUser;
import entity.Subscriber;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import rdbms.SourceService;
import system.Parameters;

public class IgniteSourceServiseImpl implements IgniteSourceService {
    private SourceService sourceService;
    private Parameters parameters;
    private Ignite ignite;


    public IgniteSourceServiseImpl(Ignite ignite, SourceService sourceService, Parameters parameters) {
        this.ignite = ignite;
        this.sourceService = sourceService;
        this.parameters = parameters;
    }

    @Override
    public void createCaches() {
        CacheConfiguration<Long, Subscriber> cacheCfgSubscriber = new CacheConfiguration<>();
        cacheCfgSubscriber.setCacheMode(CacheMode.PARTITIONED)
                .setName("SUBSCRIBER")
                .setSqlSchema("PUBLIC");

        try (IgniteCache<Long, Subscriber> cache = ignite.getOrCreateCache(cacheCfgSubscriber)) {
            cache.query(new SqlFieldsQuery(
                    "CREATE TABLE IF NOT EXISTS Subscriber (" +
                            " subs_key LONG PRIMARY KEY," +
                            " place VARCHAR," +
                            " name VARCHAR," +
                            " time_key DATE) " +
                            " WITH \"template=replicated, backups=1\"")).getAll();

            cache.query(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS " +
                    "idx_subscriber_time_key ON Subscriber (time_key)")).getAll();
        }

        CacheConfiguration<Long, Call> cacheCfgCall = new CacheConfiguration<>();
        cacheCfgCall.setCacheMode(CacheMode.PARTITIONED)
                .setName("CALL")
                .setSqlSchema("PUBLIC");

        try (IgniteCache<Long, Call> cache = ignite.getOrCreateCache(cacheCfgCall)) {
            cache.query(new SqlFieldsQuery(
                    "CREATE TABLE IF NOT EXISTS Call (" +
                            " id INT PRIMARY KEY," +
                            " subs_from LONG," +
                            " subs_to LONG," +
                            " dur INT," +
                            " start_time DATE) " +
                            " WITH \"template=partitioned,backups=1\"")).getAll();

            cache.query(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS " +
                    "idx_call_subscribers ON Call (subs_from, subs_to)")).getAll();

            cache.query(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS idx_call_dur ON Call (dur)")).getAll();
        }

        CacheConfiguration<Long, CarWash> cacheCfgCarWash = new CacheConfiguration<>();
        cacheCfgCarWash.setCacheMode(CacheMode.PARTITIONED)
                .setName("CARWASH")
                .setSqlSchema("PUBLIC");

        try (IgniteCache<Long, CarWash> cache = ignite.getOrCreateCache(cacheCfgCarWash)) {
            cache.query(new SqlFieldsQuery(
                    "CREATE TABLE IF NOT EXISTS CarWash (" +
                            " subs_key LONG PRIMARY KEY," +
                            " name VARCHAR," +
                            " place VARCHAR," +
                            " cunc_ind INT) " +
                            " WITH \"template=replicated, backups=1\"")).getAll();
        }

        CacheConfiguration<Long, CarWashUser> cacheCfgCarWashUser = new CacheConfiguration<>();
        cacheCfgCarWashUser.setCacheMode(CacheMode.PARTITIONED)
                .setName("CARWASHUSER")
                .setSqlSchema("PUBLIC");

        try (IgniteCache<Long, CarWashUser> cache = ignite.getOrCreateCache(cacheCfgCarWashUser)) {
            cache.query(new SqlFieldsQuery(
                    "CREATE TABLE IF NOT EXISTS CarWashUser (" +
                            " subs_key LONG PRIMARY KEY," +
                            " cunc_ind INT," +
                            " name VARCHAR)" +
                            " WITH \"template=replicated, backups=1\"")).getAll();
        }
    }

    @Override
    public void insertIntoCaches() {
        IgniteCache subscriberCache = ignite.cache("SQL_PUBLIC_SUBSCRIBER");
        IgniteCache callCache = ignite.cache("SQL_PUBLIC_CALL");
        IgniteCache carWashCache = ignite.cache("SQL_PUBLIC_CARWASH");

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
}
