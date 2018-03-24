package examples;

import com.sun.org.apache.xerces.internal.impl.xpath.XPath;
import org.apache.ignite.*;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Arrays;
import java.util.Collection;

public class IgniteExamples {
    public static void main(String[] args) {
      runComputingExample();
      runDataGridExample();
    }

    private static void runComputingExample() {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteCluster cluster = ignite.cluster();

            ClusterGroup rmts = cluster.forRemotes();

            IgniteCompute compute = ignite.compute(rmts);

            compute.broadcast(() -> System.out.println("Hello node!"));

            Collection<Integer> res = ignite.compute().apply(
                    (String w) -> {
                        System.out.println("Counting: " + w);

                        return w.length();
                    },
                    Arrays.asList("Apache Ignite Rules".split(" ")));

            int sum = res.stream().mapToInt(i -> i).sum();

            System.out.println("Total: " + sum);
        }
    }

    private static void runDataGridExample() {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteCompute compute = ignite.compute();

            CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>("cacheExample");
            cfg.setBackups(1);

            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cfg);

            int cnt = 10;

            for (int i = 0; i < cnt; i++) {
                cache.put(i, Integer.toString(i));
            }

            for (int i = 0; i < cnt; i++) {
                System.out.println("Got " + i + '=' + cache.get(i));
            }


        }
    }
}
