package api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class TreePerNeighbourhoodReducerFactory implements ReducerFactory<String, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new TreePerNeighbourhoodReducer();
    }

    /**
     * Reducer that takes the (count, sum) pair from the combiner and performs the reduction into a diameter average
     */
    private static class TreePerNeighbourhoodReducer extends Reducer<Integer, Integer>{
        private int sum;

        @Override
        public void reduce(Integer treeCount) {
            this.sum += treeCount;
        }

        @Override
        public Integer finalizeReduce() {
            return this.sum;
        }
    }
}
