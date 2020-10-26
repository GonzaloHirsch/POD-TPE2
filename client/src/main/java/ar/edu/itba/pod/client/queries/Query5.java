package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.collators.TreeNeighbourhoodPairCollator;
import api.combiners.TreePerNeighbourhoodCombinerFactory;
import api.mappers.TreeNeighbourhoodPairMapper;
import api.mappers.TreePerNeighbourhoodCountMapper;
import api.reducers.TreeNeighbourhoodPairReducerFactory;
import api.reducers.TreePerNeighbourhoodReducerFactory;
import ar.edu.itba.pod.client.Constants;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class Query5 extends GenericQuery<Integer, TreeSet<String>> {

    // Constants to be used
    private static final String QUERY_5_FIRST_JOB = "g2_FIRST_QUERY_5";
    private static final String QUERY_5_SECOND_JOB = "g2_SECOND_QUERY_5";
    private static final String OUTPUT_HEADER = "Grupo;Barrio A;Barrio B\n";

    // This function transforms results' entries into strings
    private static final Function<Map.Entry<Integer, TreeSet<String>>, String> RESULT_TO_STRING = r -> {
        List<String> neighbourhoods = new ArrayList<>(r.getValue());
        int size = neighbourhoods.size();
        StringBuilder sb = new StringBuilder();

        // Only want to print the neighbourhoods with more than 1000 trees and must list PAIRS
        if (r.getKey() >= 1000 && size >= 2) {
            for (int i = 0; i < size - 1; i++) {
                for (int j = i + 1; j < size; j++) {
                    sb.append(r.getKey()).append(";").append(neighbourhoods.get(i)).append(";").append(neighbourhoods.get(j)).append("\n");
                }
            }
        }
        return sb.toString();
    };

    public Query5(HazelcastInstance hz, String outputFile, Cities city){
        super(hz, city, Queries.QUERY_5, outputFile, OUTPUT_HEADER, RESULT_TO_STRING);
    }

    @Override
    protected ICompletableFuture<List<Map.Entry<Integer, TreeSet<String>>>> submitJob() throws ExecutionException, InterruptedException {
        Job<String, TreeRecord> firstJob = this.generateJobFromList(QUERY_5_FIRST_JOB);

        // submitting first job
        ICompletableFuture<Map<String, Integer>> futureJob = firstJob
            .mapper(new TreePerNeighbourhoodCountMapper())
            .combiner(new TreePerNeighbourhoodCombinerFactory())
            .reducer(new TreePerNeighbourhoodReducerFactory())
            .submit();

        // intermediate result
        Map<String, Integer> treePerNeighbourhoodResult = futureJob.get();

        final IMap<String, Integer> hzMap = this.hz.getMap(Constants.NEIGHBOURHOOD_TREE_COUNT_MAP + this.city.getValue());
        // adds data to Hazelcast IMap
        for (Map.Entry<String, Integer> r : treePerNeighbourhoodResult.entrySet()) {
            hzMap.put(r.getKey(), r.getValue());
        }

        Job<String, Integer> secondJob = this.generateJobFromMap(QUERY_5_SECOND_JOB);

        // submitting final job
        return secondJob
            .mapper(new TreeNeighbourhoodPairMapper())
            .reducer(new TreeNeighbourhoodPairReducerFactory())
            .submit(new TreeNeighbourhoodPairCollator());
    }
}
