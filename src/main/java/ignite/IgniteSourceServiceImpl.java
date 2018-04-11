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

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class IgniteSourceServiceImpl implements IgniteSourceService {
    private SourceService sourceService;
    private Parameters parameters;
    private Ignite ignite;


    public IgniteSourceServiceImpl(Ignite ignite, SourceService sourceService, Parameters parameters) {
        this.ignite = ignite;
        this.sourceService = sourceService;
        this.parameters = parameters;
    }

    private <K, T> void createCache(Class valueClass, Map<K, T> data) {
        CacheConfiguration<K, T> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setCacheMode(CacheMode.PARTITIONED)
                .setName(valueClass.getSimpleName().toUpperCase())
                .setIndexedTypes(Long.class, valueClass)
                .setSqlSchema("PUBLIC");

        try (IgniteCache<K, T> cache = ignite.getOrCreateCache(cacheCfg)) {
            cache.clear();
            cache.putAll(data);
        }
    }

    @Override
    public void createCachesAndInsert() {
        Map<Long, Subscriber> subscribers = sourceService.getSubscribers().stream()
                .collect(Collectors.toMap(Subscriber::getSubsKey, subscriber -> subscriber));
        createCache(Subscriber.class, subscribers);

        Map<Long, Call> calls =  sourceService
                .getCalls(parameters.today.atStartOfDay().minusWeeks(2), parameters.today.atStartOfDay())
                .stream().collect(Collectors.toMap(call -> call.id, call -> call));
        createCache(Call.class, calls);

        Map<Long, CarWash> carWashes = sourceService.getCarWashes().stream()
                .collect(Collectors.toMap(CarWash::getSubsKey, carWash -> carWash));
        createCache(CarWash.class, carWashes);

        createCache(CarWashUser.class, Collections.<Long, CarWashUser>emptyMap());
    }
}
