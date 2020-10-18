package ar.edu.itba.pod.client.queries;

import java.util.concurrent.ExecutionException;

public interface Query {
    /**
     * Method to execute the implemented query
     * @throws ExecutionException if the computation of the Map-Reduce job threw an exception
     * @throws InterruptedException if the computation of the Map-Reduce job threw an exception
     */
    void executeQuery() throws ExecutionException, InterruptedException;
}
