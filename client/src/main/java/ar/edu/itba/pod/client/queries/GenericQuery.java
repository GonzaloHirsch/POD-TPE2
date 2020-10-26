package ar.edu.itba.pod.client.queries;

import api.TreeRecord;
import ar.edu.itba.pod.client.Constants;
import ar.edu.itba.pod.client.CustomLogger;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.lang3.NotImplementedException;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public abstract class GenericQuery<K, V> {
    // Query properties and variables
    protected HazelcastInstance hz;
    protected Cities city;
    protected Queries query;
    protected String outputFolder;

    // Needed for output file
    protected String outputHeader;
    protected Function<Map.Entry<K, V>, String> resultToString;

    public GenericQuery(HazelcastInstance hz, Cities city, Queries query, String outputFolder, String outputHeader,
                        Function<Map.Entry<K, V>, String> resultToString) {
        this.hz = hz;
        this.city = city;
        this.query = query;
        this.outputFolder = outputFolder;
        this.outputHeader = outputHeader;
        this.resultToString = resultToString;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                        EXPOSED METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public void executeQuery() throws ExecutionException, InterruptedException {
        // Logging start time of the job
        this.logStartTime();

        // Generating the mapreduce job
        ICompletableFuture<List<Map.Entry<K, V>>> futureJob = this.submitJob();

        // Extract the desire results
        List<Map.Entry<K, V>> list = futureJob.get();

        // Generate the output string
        String infoForFile = this.prepareOutput(list);

        // Writing the results in the output file
        this.write(this.outputFolder + "/" + this.query.get_outFilename(), infoForFile);

        // Logging end time of the job
        this.logEndTime();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                        PRIVATE METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    //ICompletableFuture<List<Map.Entry<String, List<String>>>>
    protected ICompletableFuture<List<Map.Entry<K, V>>> submitJob() throws ExecutionException, InterruptedException {
        throw new NotImplementedException("This method MUST be implemented in all queries");
    }

    /**
     * Generates a Map-Reduce job for the query to be executed
     *
     * @return the ICompletableFuture object to be waited asynchronously or synchronously
     */
    protected Job<String, TreeRecord> generateJobFromList(String queryJob) {
        // Getting the job tracker
        JobTracker jobTracker = this.hz.getJobTracker(queryJob);

        // Get the list from hazelcast, we construct the name of the record based on the city name
        final IList<TreeRecord> list = this.hz.getList(Constants.TREE_RECORD_LIST + this.city.getValue());

        // Get the source for the job
        final KeyValueSource<String, TreeRecord> source = KeyValueSource.fromList(list);

        // Creating the job with the source

        return jobTracker.newJob(source);
    }

    /**
     * Generates a Map-Reduce job for the query to be executed
     *
     * @return the ICompletableFuture object to be waited asynchronously or synchronously
     */
    protected Job<String, Integer> generateJobFromMap(String queryJob) {
        // Getting the job tracker
        JobTracker jobTracker = this.hz.getJobTracker(queryJob);

        // Get the list from hazelcast, we construct the name of the record based on the city name
        final IMap<String, Integer> map = this.hz.getMap(Constants.NEIGHBOURHOOD_TREE_COUNT_MAP + this.city.getValue());

        // Get the source for the job
        final KeyValueSource<String, Integer> source = KeyValueSource.fromMap(map);

        // Creating the job with the source

        return jobTracker.newJob(source);
    }

    /**
     * Given the filtered output for the query, transform it into a string to be written to the output file
     *
     * @param results List with Map.Entry objects holding the first n results
     * @return a String with the information to be written in the output file
     */
    private String prepareOutput(List<Map.Entry<K, V>> results) {
        // We build the string with a string builder
        StringBuilder sb = new StringBuilder();
        // Adding a header
        sb.append(outputHeader);
        // Add data
        results.stream().map(resultToString).forEach(sb::append);

        return sb.toString();
    }

    /**
     * Writes a given value into a filename location(can be a path)
     *
     * @param filename path to the output file
     * @param value    value to be written to the file
     */
    protected void write(String filename, String value) {
        CustomLogger.GetInstance().write(filename, value);
    }

    /**
     * Logs the initial time of the mapreduce job as specified
     */
    protected void logStartTime() {
        CustomLogger.GetInstance().writeTimestamp(
                this.outputFolder + "/" + this.query.get_logFilename(),
                "Inicio del trabajo map/reduce",
                true
        );
        System.out.print("Inicio del trabajo map/reduce...");
    }

    /**
     * Logs the end time of the mapreduce job as specified
     */
    protected void logEndTime() {
        CustomLogger.GetInstance().writeTimestamp(
                this.outputFolder + "/" + this.query.get_logFilename(),
                "Fin del trabajo map/reduce",
                true
        );
        System.out.print(" Fin del trabajo map/reduce\n");
    }
}
