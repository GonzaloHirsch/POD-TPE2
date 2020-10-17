package api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Reducer factory for the tree diameter query, takes the Pair<Long, Double> of the (count, sum) and reduces it
 */
public class TreeDiameterReducerFactory implements ReducerFactory<String, Pair<Long, Double>, Double> {
    @Override
    public Reducer<Pair<Long, Double>, Double> newReducer(String s) {
        return new TreeDiameterReducer();
    }

    /**
     * Reducer that takes the (count, sum) pair from the combiner and performs the reduction into a diameter average
     */
    private static class TreeDiameterReducer extends Reducer<Pair<Long, Double>, Double>{
        private double sum;
        private long count;

        @Override
        public void reduce(Pair<Long, Double> tuple) {
            this.sum += tuple.getRight();
            this.count += tuple.getLeft();
        }

        @Override
        public Double finalizeReduce() {
            return this.sum / this.count;
        }
    }
}
