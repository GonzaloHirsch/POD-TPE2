package api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TreePerStreetCombinerFactory implements CombinerFactory<String, String, Map<String, AtomicLong>> {
    @Override
    public Combiner<String, Map<String, AtomicLong>> newCombiner(String s) {
        return new TreePerStreetCombiner();
    }

    private static class TreePerStreetCombiner extends Combiner<String, Map<String, AtomicLong>> {
        private Map<String, AtomicLong> treesPerStreet = new HashMap();

        @Override
        public void combine(String street) {
            if(!treesPerStreet.containsKey(street)) {
                synchronized (treesPerStreet) {
                    treesPerStreet.computeIfAbsent(street, s -> new AtomicLong(0L));
                }
            }
            treesPerStreet.get(street).getAndIncrement();
        }

        @Override
        public Map<String, AtomicLong> finalizeChunk() {
            return treesPerStreet;
        }

        @Override
        public void reset() {
            treesPerStreet = new HashMap<>();
        }
    }
}
