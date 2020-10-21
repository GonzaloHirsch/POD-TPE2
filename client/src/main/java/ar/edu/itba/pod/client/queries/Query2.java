package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.combiners.TreePerStreetCombinerFactory;
import api.mappers.TreePerStreetMapper;
import api.reducers.TreePerStreetReducerFactory;
import ar.edu.itba.pod.client.Constants;
import ar.edu.itba.pod.client.enums.Cities;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query2 extends Query {

    // Query properties and variables
    private final HazelcastInstance hz;
    private final Cities city;
    private final String outputFile;

    // Constants to be used
    private static final String QUERY_2_JOB = "QUERY_2";
    private static final String OUTPUT_HEADER = "BARRIO;CALLE_CON_MAS_ARBOLES;ARBOLES\n";

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // Alphabetical order on keys
    private static final Comparator<Map.Entry<String, MutablePair<String, Long>>> ENTRY_COMPARATOR = Comparator.comparing(Map.Entry::getKey);

    public Query2(HazelcastInstance hz, String outputFile, Cities city){
        this.hz = hz;
        this.outputFile = outputFile;
        this.city = city;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                        EXPOSED METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void executeQuery() throws ExecutionException, InterruptedException {
        // Logging start time of the job
        this.logStartTime();

        // Generating the mapreduce job
        ICompletableFuture<Map<String, MutablePair<String, Long>>> futureJob = this.generateJob();

        // Wait and retrieve the result
        Map<String, MutablePair<String, Long>> result = futureJob.get();

        // Extract the desire results
        List<Map.Entry<String, MutablePair<String, Long>>> list = this.extractResults(result);

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

    /**
     * Generates a Map-Reduce job for the query to be executed
     * @return the ICompletableFuture object to be waited asynchronously or synchronously
     */
    private ICompletableFuture<Map<String, MutablePair<String, Long>>> generateJob(){
        // Getting the job tracker
        JobTracker jobTracker = this.hz.getJobTracker(QUERY_2_JOB);

        // Get the list from hazelcast, we construct the name of the record based on the city name
        final IList<TreeRecord> list = this.hz.getList(Constants.TREE_RECORD_LIST + this.city.getValue());

        // Get the source for the job
        final KeyValueSource<String, TreeRecord> source = KeyValueSource.fromList(list);

        // Creating the job with the source
        Job<String, TreeRecord> job = jobTracker.newJob(source);

        // Setting up the job
        return job
                .mapper(new TreePerStreetMapper())
                .combiner(new TreePerStreetCombinerFactory())
                .reducer(new TreePerStreetReducerFactory())
                .submit();
    }

    /**
     * Extracts the first n results given a Map-Reduce job results
     * @param result Map with the results of the Map-Reduce job
     * @return a sorted list of map entries with the information to be written in the output file
     */
    private List<Map.Entry<String, MutablePair<String, Long>>> extractResults(Map<String, MutablePair<String, Long>> result){
        // TreeSet to get ordered results, by alphabetic neighbourhood
        TreeSet<Map.Entry<String, MutablePair<String, Long>>> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);
        orderedResults.addAll(result.entrySet());

        return new ArrayList<>(orderedResults);
    }

    /**
     * Given the filtered output for the query, transform it into a string to be written to the output file
     * @param results List with Map.Entry objects holding the first n results
     * @return a String with the information to be written in the output file
     */
    private String prepareOutput(List<Map.Entry<String, MutablePair<String, Long>>> results){
        // We build the string with a string builder
        StringBuilder sb = new StringBuilder();
        // Adding a header
        sb.append(OUTPUT_HEADER);
        // Adding the data
        results.forEach(
                r -> sb.append(r.getKey()).append(";")
                .append(r.getValue().left).append(";")
                .append(r.getValue().right).append("\n"));

        return sb.toString();
    }
}
