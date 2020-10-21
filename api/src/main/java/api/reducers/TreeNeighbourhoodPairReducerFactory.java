package api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Comparator;
import java.util.TreeSet;

public class TreeNeighbourhoodPairReducerFactory implements ReducerFactory<Integer, String, TreeSet<String>> {
    @Override
    public Reducer<String, TreeSet<String>> newReducer(Integer c) {
        return new TreeNeighbourhoodPairReducer();
    }

    /**
     * Reducer that takes the (count, sum) pair from the combiner and performs the reduction into a diameter average
     */
    private static class TreeNeighbourhoodPairReducer extends Reducer<String, TreeSet<String>>{
        TreeSet<String> neighbourhoods = new TreeSet<>(Comparator.naturalOrder());

        @Override
        public void reduce(String neighbourhood) {
            this.neighbourhoods.add(neighbourhood);
        }

        @Override
        public TreeSet<String> finalizeReduce() {
            return this.neighbourhoods;
        }
    }
}