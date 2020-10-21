package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.collators.TreeDiameterCollator;
import api.combiners.TreeDiameterCombinerFactory;
import api.mappers.TreeDiameterMapper;
import api.reducers.TreeDiameterReducerFactory;
import ar.edu.itba.pod.client.enums.Cities;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class Query3 extends GenericQuery<String, Double> {

    private int n;

    // Constants to be used
    private static final String QUERY_3_JOB = "QUERY_3";
    private static final String OUTPUT_HEADER = "NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO\n";

    private static final Function<Map.Entry<String, Double>, String> RESULT_TO_STRING =
            r -> r.getKey() + ";" + String.format(Locale.ENGLISH, "%.2f\n", r.getValue());

    public Query3(HazelcastInstance hz, String outputFile, Cities city, int n){
        super(hz, city, outputFile, OUTPUT_HEADER, RESULT_TO_STRING, null);
        this.n = n;
    }

    @Override
    public void executeQuery() throws ExecutionException, InterruptedException {
        // Logging start time of the job
        this.logStartTime();

        // Extract the desire results
        List<Map.Entry<String, Double>> list = this.customSubmitJob().get();

        // Generate the output string
        String infoForFile = this.prepareOutput(list);

        // Writing the results in the output file
        this.write(this.outputFile, infoForFile);

        // Logging end time of the job
        this.logEndTime();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                        PRIVATE METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////


    protected ICompletableFuture<List<Map.Entry<String, Double>>> customSubmitJob(){
        // Creating the job with the source
        Job<String, TreeRecord> job = this.generateJobFromList(QUERY_3_JOB);

        // Setting up the job
        return job
            .mapper(new TreeDiameterMapper())
            .combiner(new TreeDiameterCombinerFactory())
            .reducer(new TreeDiameterReducerFactory())
            .submit(new TreeDiameterCollator(this.n));
    }

    /**
     * Given the filtered output for the query, transform it into a string to be written to the output file
     * @param results List with Map.Entry objects holding the first n results
     * @return a String with the information to be written in the output file
     */
    private String prepareOutput(List<Map.Entry<String, Double>> results){
        // We build the string with a string builder
        StringBuilder sb = new StringBuilder();
        // Adding a header
        sb.append(OUTPUT_HEADER);
        // Adding the data
        results.forEach(r -> sb.append(r.getKey()).append(";").append(String.format(Locale.ENGLISH, "%.2f\n", r.getValue())));
        return sb.toString();
    }
}
