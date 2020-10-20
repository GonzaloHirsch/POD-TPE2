package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.combiners.TreeDiameterCombinerFactory;
import api.mappers.TreeDiameterMapper;
import api.reducers.TreeDiameterReducerFactory;
import ar.edu.itba.pod.client.Constants;
import ar.edu.itba.pod.client.enums.Cities;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query3 extends Query{
    // Query properties and variables
    private final HazelcastInstance hz;
    private final Cities city;
    private final int n;
    private final String outputFile;

    // Constants to be used
    private static final String QUERY_3_JOB = "QUERY_3";
    private static final String OUTPUT_HEADER = "NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO\n";

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // First comparison is made by value, in descending order
    // Second comparison is in alphabetic order
    private static final Comparator<Map.Entry<String, Double>> ENTRY_COMPARATOR = (o1, o2) -> {
        int res = o2.getValue().compareTo(o1.getValue());
        return res == 0 ? o1.getKey().compareTo(o2.getKey()) : res;
    };

    public Query3(HazelcastInstance hz, String outputFile, Cities city, int n){
        this.hz = hz;
        this.outputFile = outputFile;
        this.city = city;
        this.n = n;
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                        EXPOSED METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void executeQuery() throws ExecutionException, InterruptedException {
        // Logging start time of the job
        this.logStartTime();

        // Generating the mapreduce job
        ICompletableFuture<Map<String, Double>> futureJob = this.generateJob();

        // Wait and retrieve the result
        Map<String, Double> result = futureJob.get();

        // Extract the desire results
        List<Map.Entry<String, Double>> firstNResults = this.extractResults(result);

        // Generate the output string
        String infoForFile = this.prepareOutput(firstNResults);

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
    private ICompletableFuture<Map<String, Double>> generateJob(){
        // Getting the job tracker
        JobTracker jobTracker = this.hz.getJobTracker(QUERY_3_JOB);

        // Get the list from hazelcast, we construct the name of the record based on the city name
        final IList<TreeRecord> list = this.hz.getList(Constants.TREE_RECORD_LIST + this.city.getValue());

        // Get the source for the job
        final KeyValueSource<String, TreeRecord> source = KeyValueSource.fromList(list);

        // Creating the job with the source
        Job<String, TreeRecord> job = jobTracker.newJob(source);

        // Setting up the job
        return job
                .mapper(new TreeDiameterMapper())
                .combiner(new TreeDiameterCombinerFactory())
                .reducer(new TreeDiameterReducerFactory())
                .submit();
    }

    /**
     * Extracts the first n results given a Map-Reduce job results
     * @param result Map with the results of the Map-Reduce job
     * @return a sorted list of map entries with the information to be written in the output file
     */
    private List<Map.Entry<String, Double>> extractResults(Map<String, Double> result){
        // TreeSet to get ordered results, first descending by diameter, then by alphabetic name
        TreeSet<Map.Entry<String, Double>> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);
        orderedResults.addAll(result.entrySet());

        // Keeping the first n results, we use an iterator for this
        List<Map.Entry<String, Double>> firstNResults = new ArrayList<>(this.n);
        Iterator<Map.Entry<String, Double>> iterator = orderedResults.iterator();
        int count = 0;
        while (count < n && iterator.hasNext()){
            firstNResults.add(iterator.next());
            count++;
        }

        return firstNResults;
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
