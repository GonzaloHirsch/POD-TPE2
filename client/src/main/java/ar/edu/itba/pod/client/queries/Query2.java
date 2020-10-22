package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.collators.TreePerStreetCollator;
import api.combiners.TreePerStreetCombinerFactory;
import api.mappers.TreePerStreetMapper;
import api.reducers.TreePerStreetReducerFactory;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.*;
import java.util.function.Function;

public class Query2 extends GenericQuery<String, MutablePair<String, Long>> {

    // Constants to be used
    private static final String QUERY_2_JOB = "QUERY_2";
    private static final String OUTPUT_HEADER = "BARRIO;CALLE_CON_MAS_ARBOLES;ARBOLES\n";

    // This function transforms results' entries into strings
    private static final Function<Map.Entry<String, MutablePair<String, Long>>, String> RESULT_TO_STRING =
            r -> r.getKey() + ";" + r.getValue().left + ";" + r.getValue().right + "\n";

    private final int min;

    public Query2(HazelcastInstance hz, String outputFile, Cities city, int min){
        super(hz, city, Queries.QUERY_2, outputFile, OUTPUT_HEADER, RESULT_TO_STRING);
        this.min = min;
    }

    /**
     * Generates a Map-Reduce job for the query to be executed
     * @return the ICompletableFuture object to be waited asynchronously or synchronously
     */
    @Override
    protected ICompletableFuture<List<Map.Entry<String, MutablePair<String, Long>>>> submitJob(){
        // Creating the job with the source
        Job<String, TreeRecord> job = this.generateJobFromList(QUERY_2_JOB);

        // Setting up the job
        return job
                .mapper(new TreePerStreetMapper())
                .combiner(new TreePerStreetCombinerFactory())
                .reducer(new TreePerStreetReducerFactory())
                .submit(new TreePerStreetCollator(min));
    }
}