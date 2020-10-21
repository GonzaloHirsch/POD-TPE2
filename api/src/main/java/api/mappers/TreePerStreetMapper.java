package api.mappers;

import api.TreeRecord;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TreePerStreetMapper implements Mapper<String, TreeRecord, String, String> {
    @Override
    public void map(String key, TreeRecord record, Context<String, String> context) {
        context.emit(record.getNeighbourhoodName(),record.getStreet());
    }
}
