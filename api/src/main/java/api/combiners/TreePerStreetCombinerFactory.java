package api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashMap;
import java.util.Map;

public class TreePerStreetCombinerFactory implements CombinerFactory<String, String, Map<String, Long>> {
    @Override
    public Combiner<String, Map<String, Long>> newCombiner(String s) {
        return new TreePerStreetCombiner();
    }

    private static class TreePerStreetCombiner extends Combiner<String, Map<String, Long>> {
        private Map<String, Long> treesPerStreet = new HashMap<>();

        @Override
        public void combine(String street) {
            if (!treesPerStreet.containsKey(street)) {
                treesPerStreet.putIfAbsent(street, 0L);
            }
            treesPerStreet.put(street, treesPerStreet.get(street) + 1);
        }

        @Override
        public Map<String, Long> finalizeChunk() {
            return treesPerStreet;
        }

        @Override
        public void reset() {
            treesPerStreet = new HashMap<>();
        }
    }
}
