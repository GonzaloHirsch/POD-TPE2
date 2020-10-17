package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.enums.Queries;
import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String NAME = "tpe2-g2";
    private static final String PASSWORD = "nuestra_password";

    public static void main(String[] args) {
        try {
            // Creating the arguments object
            ClientArguments arguments = new ClientArguments();

            // Parsing the arguments
            try {
                arguments.parseArguments();
            } catch (InvalidArgumentsException e) {
                System.out.println(e.getMessage());
            }

            // Creating an instance of the Hazelcast Client
            final HazelcastInstance hz = GetHazelInstance(arguments.getAddresses());

            // File parsing to get both neighbours and tree records information
            Parser parser = new Parser(arguments.getCity(), arguments.getInPath());
            parser.parse();
            Map<String, Long> neighbours = parser.getNeighbours();
            List<TreeRecord> treeRecords = parser.getTreeRecords();

            // TODO: TODA LA LOGICA VA ACA
            // TODO: AGREGAR CODIGO POR QUERY
            // TODO: SE PUEDEN HACER FUNCIONES QUE RECIBAN LOS PARAMETROS DE ARGUMENTS Y EL CLIENTE DE HAZELCAST
            Queries chosenQuery = arguments.getQuery();
            switch (chosenQuery) {
                case QUERY_1:
                    break;
                case QUERY_2:
                    break;
                case QUERY_3:
                    break;
                case QUERY_4:
                    break;
                case QUERY_5:
                    break;
            }
        } catch (IOException e) {
            System.out.println("ERROR: There was a problem while parsing files");
        } catch (Exception e) {
            // FIXME: BETTER ERRORS HERE
            System.out.println("ERROR: Exception in the server");
        }
    }

    /**
     * Creates an instance of the Hazelcast Client based on a collection of ips + ports for the members.
     * <br><br>
     * Group config and network config example from: https://www.codota.com/code/java/classes/com.hazelcast.client.config.ClientConfig
     *
     * @param memberIPs Collection of IP:PORT strings for each member
     * @return an instance of a Hazelcast Client
     */
    private static HazelcastInstance GetHazelInstance(Collection<String> memberIPs) {
        // Instancing the configuration
        ClientConfig config = new ClientConfig();

        // Setting the name and password for the target cluster
        GroupConfig groupConfig = config.getGroupConfig();
        groupConfig.setName(NAME).setPassword(PASSWORD);

        // Adding all the IPs to the network configuration
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        memberIPs.forEach(networkConfig::addAddress);

        return HazelcastClient.newHazelcastClient(config);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                          QUERIES
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private static void performQuery3(HazelcastInstance hz, int n) throws ExecutionException, InterruptedException {
        // Getting the job tracker
        // FIXME: CORRECT NAME
        JobTracker jobTracker = hz.getJobTracker("word-count");

        // Get the list from hazelcast
        // FIXME: CORRECT GENERIC TYPE AND KEY
        final IList<String> list = hz.getList("MYLIST");

        // Get the source for the job
        // FIXME: CORRECT GENERIC TYPE AND KEY
        final KeyValueSource<String, String> source = KeyValueSource.fromList(list);

        // Creating the job with the source
        // FIXME: CORRECT GENERIC TYPE AND KEY
        Job<String, String> job = jobTracker.newJob(source);

        // FIXME: PUT CORRECT TYPES
        /*// Setting up the job
        ICompletableFuture<Map<String, Double>> futureJob = job
                .mapper(new TreeDiameterMapper())
                .combiner(new TreeDiameterCombinerFactory())
                .reducer(new TreeDiameterReducerFactory())
                .submit();

        // Attach a callback listener
        futureJob.andThen(buildCallback());

        // Wait and retrieve the result
        Map<String, Double> result = futureJob.get();*/
    }
}
