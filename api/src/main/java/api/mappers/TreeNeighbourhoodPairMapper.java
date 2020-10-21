package api.mappers;

import api.TreeRecord;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TreeNeighbourhoodPairMapper implements Mapper<String, Integer, Integer, String> {

    @Override
    public void map(String neighbourhood, Integer treeCount, Context<Integer, String> context) {
        // Round down to the nearest thousand
        int roundedTreeCount = treeCount - (treeCount % 1000);
        context.emit(roundedTreeCount, neighbourhood);
    }
}

