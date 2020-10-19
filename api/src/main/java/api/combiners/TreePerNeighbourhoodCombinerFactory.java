package api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/**
 * Combiner factory to generate TreePerCityCombiners to optimize the job.
 * <br><br>
 * It combines the 1 values for the tree count into a sum
 */
public class TreePerNeighbourhoodCombinerFactory implements CombinerFactory<String, Integer, Integer> {
    @Override
    public Combiner<Integer, Integer> newCombiner(String s) {
        return new TreePerNeighbourhoodCombiner();
    }

    private static class TreePerNeighbourhoodCombiner extends Combiner<Integer, Integer> {
        private int count = 0;

        @Override
        public void combine(Integer addTree) {
            this.count++;
        }

        @Override
        public Integer finalizeChunk() {
            return this.count;
        }

        @Override
        public void reset() {
            this.count = 0;
        }
    }
}
