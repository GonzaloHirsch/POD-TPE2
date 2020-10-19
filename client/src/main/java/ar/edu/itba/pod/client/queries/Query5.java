package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import api.combiners.TreeDiameterCombinerFactory;
import api.combiners.TreePerNeighbourhoodCombinerFactory;
import api.mappers.TreeDiameterMapper;
import api.mappers.TreeNeighbourhoodPairMapper;
import api.mappers.TreePerNeighbourhoodCountMapper;
import api.reducers.TreeDiameterReducerFactory;
import api.reducers.TreeNeighbourhoodPairReducerFactory;
import api.reducers.TreePerNeighbourhoodReducerFactory;
import ar.edu.itba.pod.client.Client;
import ar.edu.itba.pod.client.Constants;
import ar.edu.itba.pod.client.enums.Cities;
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
import java.util.stream.Collectors;

public class Query5 implements Query {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    // Query properties and variables
    private final HazelcastInstance hz;
    private final Cities city;
    private final String outputFile;

    // Constants to be used
    private static final String QUERY_5_FIRST_JOB = "FIRST_QUERY_5";
    private static final String QUERY_5_SECOND_JOB = "SECOND_QUERY_5";
    private static final String OUTPUT_HEADER = "Grupo;Barrio A;Barrio B\n";

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // First comparison is made by value, in descending order
    // Second comparison is in alphabetic order
    private static final Comparator<Map.Entry<Integer, TreeSet<String>>> ENTRY_COMPARATOR = Comparator.comparing(Map.Entry::getKey);

    public Query5(HazelcastInstance hz, String outputFile, Cities city){
        this.hz = hz;
        this.outputFile = outputFile;
        this.city = city;
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                        EXPOSED METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void executeQuery() throws ExecutionException, InterruptedException {
        // Generating the first mapreduce job
        ICompletableFuture<Map<String, Integer>> futureTreeCountJob = this.generateTreeCountJob();

        // Wait and retrieve the result
        Map<String, Integer> treePerNeighbourhoodResult = futureTreeCountJob.get();

        // Generating the second mapreduce job
        ICompletableFuture<Map<Integer, TreeSet<String>>> futureFinalJob = this.generateTreeNeighbourhoodPairJob(treePerNeighbourhoodResult);

        // Wait and retrieve the result
        Map<Integer, TreeSet<String>> treeNeighbourhoodPairResults = futureFinalJob.get();

        // Order results by tree amount
        TreeSet<Map.Entry<Integer, TreeSet<String>>> sortedResults = this.sortResults(treeNeighbourhoodPairResults);

        // Generate the output string
        String infoForFile = this.prepareOutput(sortedResults);

        // Writing the results in the output file
        this.write(this.outputFile, infoForFile);
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                        PRIVATE METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Generates a Map-Reduce job to count the trees per neighbourhood
     * @return the ICompletableFuture object to be waited asynchronously or synchronously
     */
    private ICompletableFuture<Map<String, Integer>> generateTreeCountJob() {
        // Getting the job tracker
        JobTracker jobTracker = this.hz.getJobTracker(QUERY_5_FIRST_JOB);

        // Get the list from hazelcast, we construct the name of the record based on the city name
        final IList<TreeRecord> list = this.hz.getList(Constants.TREE_RECORD_LIST + this.city.getValue());

        // TODO Delete this once parser is done correctly
        TreeRecord tr1 = new TreeRecord("Hola", "calle falsa", "mi arbolito", 10);
        TreeRecord tr2 = new TreeRecord("Hola", "calle falsa", "mi arbolito 1", 20);
        TreeRecord tr3 = new TreeRecord("Hola", "calle falsa", "mi arbolito 1", 40);
        TreeRecord tr4 = new TreeRecord("Hola", "calle falsa", "mi arbolito", 15);
        TreeRecord tr5 = new TreeRecord("Hola", "calle falsa", "mi arbolito 2", 30);
        list.addAll(Arrays.asList(tr1, tr2, tr3, tr4, tr5));

        for (int i = 0; i < 2500; i++) {
            TreeRecord b1 = new TreeRecord("Barrio 1", "calle falsa", "mi arbolito", 10);
            TreeRecord b2 = new TreeRecord("Barrio 2", "calle falsa", "mi arbolito", 10);
            TreeRecord b3 = new TreeRecord("Barrio 3", "calle falsa", "mi arbolito", 10);
            TreeRecord b4 = new TreeRecord("Barrio 4", "calle falsa", "mi arbolito", 10);
            list.addAll(Arrays.asList(b1, b2, b2, b3, b2, b4, b1, b1));
        }

        // Get the source for the job
        final KeyValueSource<String, TreeRecord> source = KeyValueSource.fromList(list);

        // Creating the job with the source
        Job<String, TreeRecord> job = jobTracker.newJob(source);

        // Setting up the job
        return job
                .mapper(new TreePerNeighbourhoodCountMapper())
                .combiner(new TreePerNeighbourhoodCombinerFactory())
                .reducer(new TreePerNeighbourhoodReducerFactory())
                .submit();
    }

    /**
     * Generates a Map-Reduce job for the query to be executed
     * @return the ICompletableFuture object to be waited asynchronously or synchronously
     */
    private ICompletableFuture<Map<Integer, TreeSet<String>>> generateTreeNeighbourhoodPairJob(Map<String, Integer> treePerNeighbourhoodResult) {
        // Getting the job tracker
        JobTracker jobTracker = this.hz.getJobTracker(QUERY_5_SECOND_JOB);

        // Passing the Map of the first MapReduce to IMap to use as source for next MapReduce
        IMap<String, Integer> map = this.hz.getMap(Constants.NEIGHBOURHOOD_TREE_COUNT_MAP + this.city.getValue());
        for (Map.Entry<String, Integer> entry: treePerNeighbourhoodResult.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }

        // Get the source for the job
        final KeyValueSource<String, Integer> source = KeyValueSource.fromMap(map);

        // Creating the job with the source
        Job<String, Integer> job = jobTracker.newJob(source);

        // Setting up the job
        return job
                .mapper(new TreeNeighbourhoodPairMapper())
                .reducer(new TreeNeighbourhoodPairReducerFactory())
                .submit();
    }

    /**
     * Sorts the result based on the key (tree amount)
     * @param result Map with the results of the Map-Reduce jobs
     * @return a sorted list of map entries with the information to be written in the output file
     */
    private TreeSet<Map.Entry<Integer, TreeSet<String>>> sortResults(Map<Integer, TreeSet<String>> result){
        // TreeSet to order by tree amount
        TreeSet<Map.Entry<Integer, TreeSet<String>>> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);
        orderedResults.addAll(result.entrySet());
        return orderedResults;
    }

    /**
     * Given the filtered output for the query, transform it into a string to be written to the output file
     * @param results List with Map.Entry objects
     * @return a String with the information to be written in the output file
     */
    private String prepareOutput(TreeSet<Map.Entry<Integer, TreeSet<String>>> results){
        // We build the string with a string builder
        StringBuilder sb = new StringBuilder();
        // Adding a header
        sb.append(OUTPUT_HEADER);
        // Adding the data
        results.forEach(pair -> sb.append(this.formatKeyValuePair(pair)));
        return sb.toString();
    }
    /**
     * Given the filtered output for the query, transform it into a string to be written to the output file
     * @param pair Map.Entry of the key value to format into a string
     * @return a String with the information of the pair
     */
    private StringBuilder formatKeyValuePair(Map.Entry<Integer, TreeSet<String>> pair) {
        List<String> neighbourhoods = new ArrayList<>(pair.getValue());
        int size = neighbourhoods.size();
        StringBuilder sb = new StringBuilder();

        // Only want to print the neighbourhoods with more than 1000 trees and must list PAIRS
        if (pair.getKey() >= 1000 && size >= 2) {
            for (int i = 0; i < size - 1; i++) {
                for (int j = i + 1; j < size; j++) {
                    sb.append(pair.getKey()).append(";").append(neighbourhoods.get(i)).append(";").append(neighbourhoods.get(j)).append("\n");
                }
            }
        }
        return sb;
    }

    /**
     * Writes a given value into a filename location(can be a path)
     * @param filename path to the output file
     * @param value value to be written to the file
     */
    private void write(String filename, String value) {
        try {
            FileWriter myWriter = new FileWriter(filename);
            myWriter.write(value);
            myWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred when writing query 5 to " + filename);
        }
    }
}
