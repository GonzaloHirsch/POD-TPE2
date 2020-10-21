package api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TreePerStreetReducerFactory implements ReducerFactory<String, Map<String, AtomicLong>, MutablePair<String, Long>> {
    @Override
    public Reducer<Map<String, AtomicLong>, MutablePair<String, Long>> newReducer(String key) {
        return new TreePerStreetReducer();
    }

    private class TreePerStreetReducer extends Reducer<Map<String, AtomicLong>, MutablePair<String, Long>>{
        private Map<String, AtomicLong> trees;
        @Override
        public void beginReduce() {
            trees = new HashMap<>();
        }

        @Override
        public void reduce(Map<String, AtomicLong> value) {
            for(Map.Entry<String, AtomicLong> entry : value.entrySet()){
                synchronized (trees) {
                    trees.computeIfAbsent(entry.getKey(), s -> new AtomicLong(0));
                }
                trees.get(entry.getKey()).getAndAdd(entry.getValue().get());
            }
        }

        @Override
        public MutablePair<String, Long> finalizeReduce() {
            Map.Entry<String, AtomicLong> maxEntry = Collections.max(trees.entrySet(), Comparator.comparingLong(e -> e.getValue().get()));
            return new MutablePair<>(maxEntry.getKey(), maxEntry.getValue().get());
        }
    }
}
