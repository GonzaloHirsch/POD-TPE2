package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.collators.TreeDiameterCollator;
import api.combiners.TreeDiameterCombinerFactory;
import api.mappers.TreeDiameterMapper;
import api.reducers.TreeDiameterReducerFactory;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.util.*;
import java.util.function.Function;

public class Query3 extends GenericQuery<String, Double> {
    private final int n;

    // Constants to be used
    private static final String QUERY_3_JOB = "QUERY_3";
    private static final String OUTPUT_HEADER = "NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO\n";

    private static final Function<Map.Entry<String, Double>, String> RESULT_TO_STRING =
            r -> r.getKey() + ";" + String.format(Locale.ENGLISH, "%.2f\n", r.getValue());

    public Query3(HazelcastInstance hz, String outputFile, Cities city, int n){
        super(hz, city, Queries.QUERY_3, outputFile, OUTPUT_HEADER, RESULT_TO_STRING);
        this.n = n;
    }

    @Override
    protected ICompletableFuture<List<Map.Entry<String, Double>>> submitJob(){
        // Creating the job with the source
        Job<String, TreeRecord> job = this.generateJobFromList(QUERY_3_JOB);

        // Setting up the job
        return job
            .mapper(new TreeDiameterMapper())
            .combiner(new TreeDiameterCombinerFactory())
            .reducer(new TreeDiameterReducerFactory())
            .submit(new TreeDiameterCollator(this.n));
    }
}
