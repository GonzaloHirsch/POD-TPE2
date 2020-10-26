package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.collators.TreePerNeighbourhoodCollator;
import api.combiners.TreePerNeighbourhoodCombinerFactory;
import api.mappers.TreePerNeighbourhoodCountMapper;
import api.reducers.TreePerNeighbourhoodReducerFactory;
import ar.edu.itba.pod.client.Constants;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class Query1 extends GenericQuery<String, Double> {

    // Constants to be used
    private static final String QUERY_1_JOB = "g2_QUERY_1";
    private static final String OUTPUT_HEADER = "BARRIO;ARBOLES_POR_HABITANTE\n";

    // This function transforms results' entries into strings
    private static final Function<Map.Entry<String, Double>, String> RESULT_TO_STRING =
            r -> r.getKey() + ";" + String.format(Locale.ENGLISH, "%.2f\n", r.getValue());

    public Query1(HazelcastInstance hz, String outputFile, Cities city){
        super(hz, city, Queries.QUERY_1, outputFile, OUTPUT_HEADER, RESULT_TO_STRING);
    }

    /**
     * Generates a Map-Reduce job for the query to be executed
     * @return the ICompletableFuture object to be waited asynchronously or synchronously
     */
    @Override
    protected ICompletableFuture<List<Map.Entry<String, Double>>> submitJob(){
        // Creating the job with the source
        Job<String, TreeRecord> job = this.generateJobFromList(QUERY_1_JOB);

        // Setting up the job
        return job
            .mapper(new TreePerNeighbourhoodCountMapper())
            .combiner(new TreePerNeighbourhoodCombinerFactory())
            .reducer(new TreePerNeighbourhoodReducerFactory())
            .submit(new TreePerNeighbourhoodCollator(hz.getMap(Constants.NEIGHBOURHOOD_TREE_COUNT_MAP + this.city.getValue())));
    }
}
