package api.mappers;

import api.TreeRecord;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TreePerNeighbourhoodWithTreeNameCountMapper  implements Mapper<String, TreeRecord, String, Integer> {
    private String treeName;
    public TreePerNeighbourhoodWithTreeNameCountMapper(String treeName){
        this.treeName = treeName;
    }

    @Override
    public void map(String s, TreeRecord treeRecord, Context<String, Integer> context) {
        if(treeRecord.getCommonName().equals(treeName))
            context.emit(treeRecord.getNeighbourhoodName(), 1);
    }
}
