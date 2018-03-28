package ignite;

import entity.Call;
import entity.CarWash;
import entity.CarWashUser;
import entity.Subscriber;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import rdbms.SourceService;
import system.Parameters;

public class IgniteSourceServiceImpl implements IgniteSourceService {
    private SourceService sourceService;
    private Parameters parameters;
    private Ignite ignite;


    public IgniteSourceServiceImpl(Ignite ignite, SourceService sourceService, Parameters parameters) {
        this.ignite = ignite;
        this.sourceService = sourceService;
        this.parameters = parameters;
    }

    @Override
    public void createCachesAndInsert() {
        CacheConfiguration<Long, Subscriber> cacheCfgSubscriber = new CacheConfiguration<>();
        cacheCfgSubscriber.setCacheMode(CacheMode.PARTITIONED)
                .setName("SUBSCRIBER")
                .setIndexedTypes(Long.class, Subscriber.class)
                .setSqlSchema("PUBLIC");

        try (IgniteCache<Long, Subscriber> cache = ignite.getOrCreateCache(cacheCfgSubscriber)) {
            cache.clear();

            sourceService.getSubscribers().forEach((s) ->
                    cache.put(s.getSubsKey(), s)
            );
        }

        CacheConfiguration<Long, Call> cacheCfgCall = new CacheConfiguration<>();
        cacheCfgCall.setCacheMode(CacheMode.PARTITIONED)
                .setName("CALL")
                .setIndexedTypes(Long.class, Call.class)
                .setSqlSchema("PUBLIC");

        try (IgniteCache<Long, Call> cache = ignite.getOrCreateCache(cacheCfgCall)) {
            cache.clear();

            sourceService.getCalls(parameters.today.atStartOfDay().minusWeeks(2),
                    parameters.today.atStartOfDay()).forEach((s) ->
                    cache.put(s.id, s)
            );
        }

        CacheConfiguration<Long, CarWash> cacheCfgCarWash = new CacheConfiguration<>();
        cacheCfgCarWash.setCacheMode(CacheMode.PARTITIONED)
                .setName("CARWASH")
                .setIndexedTypes(Long.class, CarWash.class)
                .setSqlSchema("PUBLIC");

        try (IgniteCache<Long, CarWash> cache = ignite.getOrCreateCache(cacheCfgCarWash)) {
            cache.clear();

            sourceService.getCarWashes().forEach((s) ->
                    cache.put(s.getSubsKey(), s)
            );
        }

        CacheConfiguration<Long, CarWashUser> cacheCfgCarWashUser = new CacheConfiguration<>();
        cacheCfgCarWashUser.setCacheMode(CacheMode.PARTITIONED)
                .setName("CARWASHUSER")
                .setIndexedTypes(Long.class, CarWashUser.class)
                .setSqlSchema("PUBLIC");

        try (IgniteCache<Long, CarWashUser> cache = ignite.getOrCreateCache(cacheCfgCarWashUser)) {
            // NOP
        }
    }
}
