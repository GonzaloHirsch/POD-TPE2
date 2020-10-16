package api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Combiner factory to generate TreeDiameterCombiners to optimize the job.
 * <br><br>
 * It combines the different double values for the widths into a sum
 */
public class TreeDiameterCombinerFactory implements CombinerFactory<String, Double, Pair<Long, Double>> {
    @Override
    public Combiner<Double, Pair<Long, Double>> newCombiner(String s) {
        return new TreeDiameterCombiner();
    }

    private static class TreeDiameterCombiner extends Combiner<Double, Pair<Long, Double>> {
        private double sum = 0;
        private long count = 0;

        @Override
        public void combine(Double aDouble) {
            this.sum += aDouble;
            this.count++;
        }

        @Override
        public Pair<Long, Double> finalizeChunk() {
            return new ImmutablePair<>(this.count, this.sum);
        }

        @Override
        public void reset() {
            this.sum = 0;
            this.count = 0;
        }
    }
}
