package api.mappers;

import api.TreeRecord;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/**
 * Mapper Documentation: https://docs.hazelcast.org/docs/3.6.8/javadoc/com/hazelcast/mapreduce/Mapper.html
 * <br><br>
 * Mapper for the Query 3, it maps the tree name to the diameter
 */
public class TreeDiameterMapper implements Mapper<String, TreeRecord, String, Double> {

    @Override
    public void map(String s, TreeRecord treeRecord, Context<String, Double> context) {
        context.emit(treeRecord.getCommonName(), treeRecord.getDiameter());
    }
}
