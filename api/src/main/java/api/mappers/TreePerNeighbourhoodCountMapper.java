package api.mappers;

import api.TreeRecord;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/**
 * Mapper Documentation: https://docs.hazelcast.org/docs/3.6.8/javadoc/com/hazelcast/mapreduce/Mapper.html
 * <br><br>
 * Mapper for the Query 5, it maps the city name to 1
 */
public class TreePerNeighbourhoodCountMapper implements Mapper<String, TreeRecord, String, Integer> {

    @Override
    public void map(String s, TreeRecord treeRecord, Context<String, Integer> context) {
        context.emit(treeRecord.getNeighbourhoodName(), 1);
    }
}
