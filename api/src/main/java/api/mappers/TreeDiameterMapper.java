package api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/**
 * Mapper Documentation: https://docs.hazelcast.org/docs/3.6.8/javadoc/com/hazelcast/mapreduce/Mapper.html
 */
public class TreeDiameterMapper implements Mapper<String, String, String, Long> {
    @Override
    public void map(String s, String s2, Context<String, Long> context) {
        // FIXME: Here we have to emit name -> width
        context.emit(s, 1L);
    }
}
