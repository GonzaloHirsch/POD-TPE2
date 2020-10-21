package api.mappers;

import api.TreeRecord;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TreeNeighbourhoodPairMapper implements Mapper<String, Long, Long, String> {

    @Override
    public void map(String neighbourhood, Long treeCount, Context<Long, String> context) {
        // Round down to the nearest thousand
        long roundedTreeCount = treeCount - (treeCount % 1000);
        context.emit(roundedTreeCount, neighbourhood);
    }
}

