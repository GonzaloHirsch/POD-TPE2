package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.collators.Query4Collator;
import api.collators.TreeDiameterCollator;
import api.collators.TreeNeighbourhoodPairCollator;
import api.combiners.TreeDiameterCombinerFactory;
import api.combiners.TreePerNeighbourhoodCombinerFactory;
import api.mappers.TreeDiameterMapper;
import api.mappers.TreeNeighbourhoodPairMapper;
import api.mappers.TreePerNeighbourhoodCountMapper;
import api.mappers.TreePerNeighbourhoodWithTreeNameCountMapper;
import api.reducers.TreeDiameterReducerFactory;
import api.reducers.TreeNeighbourhoodPairReducerFactory;
import api.reducers.TreePerNeighbourhoodReducerFactory;
import ar.edu.itba.pod.client.Client;
import ar.edu.itba.pod.client.Constants;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class Query4 extends GenericQuery<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private final String treeName;
    private final int minTrees;

    // Constants to be used
    private static final String QUERY_4_JOB = "QUERY_4";
    private static final String OUTPUT_HEADER = "Barrio A;Barrio B\n";

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // First comparison is made by value, in descending order
    // Second comparison is in alphabetic order
    // This function transforms results' entries into strings
    private static final Function<Map.Entry<String, String>, String> RESULT_TO_STRING = r -> r.getKey() + ";" + r.getValue() + "\n";

    public Query4(HazelcastInstance hz, String outputFile, Cities city, String treeName, int minTrees){
        super(hz, city, Queries.QUERY_4, outputFile, OUTPUT_HEADER, RESULT_TO_STRING);
        this.treeName = treeName;
        this.minTrees = minTrees;
    }

    @Override
    protected ICompletableFuture<List<Map.Entry<String, String>>> submitJob() throws ExecutionException, InterruptedException {
        Job<String, TreeRecord> job = this.generateJobFromList(QUERY_4_JOB);

        return job
                .mapper(new TreePerNeighbourhoodWithTreeNameCountMapper(this.treeName))
                .combiner(new TreePerNeighbourhoodCombinerFactory())
                .reducer(new TreePerNeighbourhoodReducerFactory())
                .submit(new Query4Collator(this.minTrees));
    }
}
