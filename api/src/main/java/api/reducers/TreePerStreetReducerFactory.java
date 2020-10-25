package api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class TreePerStreetReducerFactory implements ReducerFactory<String, Map<String, Long>, MutablePair<String, Long>> {
    @Override
    public Reducer<Map<String, Long>, MutablePair<String, Long>> newReducer(String key) {
        return new TreePerStreetReducer();
    }

    private static class TreePerStreetReducer extends Reducer<Map<String, Long>, MutablePair<String, Long>> {
        private Map<String, Long> trees;

        @Override
        public void beginReduce() {
            this.trees = new HashMap<>();
        }

        @Override
        public void reduce(Map<String, Long> value) {
            for (Map.Entry<String, Long> entry : value.entrySet()) {
                this.trees.putIfAbsent(entry.getKey(), 0L);
                this.trees.put(entry.getKey(), this.trees.get(entry.getKey() + entry.getValue()));
            }
        }

        @Override
        public MutablePair<String, Long> finalizeReduce() {
            Map.Entry<String, Long> maxEntry = Collections.max(this.trees.entrySet(), Comparator.comparingLong(Map.Entry::getValue));
            return new MutablePair<>(maxEntry.getKey(), maxEntry.getValue());
        }
    }
}
