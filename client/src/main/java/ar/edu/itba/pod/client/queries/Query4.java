package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.collators.TreeMinimumPairCollator;
import api.combiners.TreePerNeighbourhoodCombinerFactory;
import api.mappers.TreePerNeighbourhoodWithTreeNameCountMapper;
import api.reducers.TreePerNeighbourhoodReducerFactory;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class Query4 extends GenericQuery<String, List<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(Query4.class);

    private final String treeName;
    private final int minTrees;

    // Constants to be used
    private static final String QUERY_4_JOB = "QUERY_4";
    private static final String OUTPUT_HEADER = "Barrio A;Barrio B\n";

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // First comparison is made by value, in descending order
    // Second comparison is in alphabetic order
    // This function transforms results' entries into strings
    private static final Function<Map.Entry<String, List<String>>, String> RESULT_TO_STRING = r -> {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < r.getValue().size(); i++) {
            sb.append(r.getKey()).append(";").append(r.getValue().get(i)).append("\n");
        }
        return sb.toString();
    };

    public Query4(HazelcastInstance hz, String outputFile, Cities city, String treeName, int minTrees) {
        super(hz, city, Queries.QUERY_4, outputFile, OUTPUT_HEADER, RESULT_TO_STRING);
        this.treeName = treeName;
        this.minTrees = minTrees;
    }

    @Override
    protected ICompletableFuture<List<Map.Entry<String, List<String>>>> submitJob() throws ExecutionException, InterruptedException {
        Job<String, TreeRecord> job = this.generateJobFromList(QUERY_4_JOB);

        return job
                .mapper(new TreePerNeighbourhoodWithTreeNameCountMapper(this.treeName))
                .combiner(new TreePerNeighbourhoodCombinerFactory())
                .reducer(new TreePerNeighbourhoodReducerFactory())
                .submit(new TreeMinimumPairCollator(this.minTrees));

    }
}
