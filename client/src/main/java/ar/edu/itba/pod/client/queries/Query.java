package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class Query {
    private static final Logger LOG = LoggerFactory.getLogger(Query.class);

    /**
     * Method to execute the implemented query
     * @throws ExecutionException if the computation of the Map-Reduce job threw an exception
     * @throws InterruptedException if the computation of the Map-Reduce job threw an exception
     */
    public abstract void executeQuery() throws ExecutionException, InterruptedException;

    /**
     * Writes a given value into a filename location(can be a path)
     * @param filename path to the output file
     * @param value value to be written to the file
     */
    protected void write(String filename, String value) {
        try {
            FileWriter myWriter = new FileWriter(filename);
            myWriter.write(value);
            myWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred when writing query 5 to " + filename);
        }
    }

    /**
     * Logs the initial time of the mapreduce job as specified
     */
    protected void logStartTime(){
        LOG.info("Inicio del trabajo map/reduce");
    }

    /**
     * Logs the end time of the mapreduce job as specified
     */
    protected void logEndTime(){
        LOG.info("Fin del trabajo map/reduce");
    }
}
