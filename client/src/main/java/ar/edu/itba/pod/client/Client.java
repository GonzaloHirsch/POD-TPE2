package ar.edu.itba.pod.client;

import api.TreeRecord;
import api.combiners.TreeDiameterCombinerFactory;
import api.mappers.TreeDiameterMapper;
import api.reducers.TreeDiameterReducerFactory;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String NAME = "tpe2-g2";
    private static final String PASSWORD = "nuestra_password";

    private static final String QUERY_3_JOB = "QUERY_3";
    private static final String TREE_RECORD_LIST = "TREE_LIST_";

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
                    performQuery3(hz, arguments.getCity(), arguments.getN());
                    break;
                case QUERY_4:
                    break;
                case QUERY_5:
                    break;
            }

            // Exit the program, we need this because it keeps the hazelcast connection open otherwise
            System.exit(0);
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

    private static void performQuery3(HazelcastInstance hz, Cities city, int n) throws ExecutionException, InterruptedException {
        // Getting the job tracker
        JobTracker jobTracker = hz.getJobTracker(QUERY_3_JOB);

        // Get the list from hazelcast, we construct the name of the record based on the city name
        final IList<TreeRecord> list = hz.getList(TREE_RECORD_LIST + city.getValue());
        TreeRecord tr1 = new TreeRecord("Hola", "calle falsa", "mi arbolito", 10);
        TreeRecord tr2 = new TreeRecord("Hola", "calle falsa", "mi arbolito 1", 20);
        TreeRecord tr3 = new TreeRecord("Hola", "calle falsa", "mi arbolito 1", 40);
        TreeRecord tr4 = new TreeRecord("Hola", "calle falsa", "mi arbolito", 15);
        TreeRecord tr5 = new TreeRecord("Hola", "calle falsa", "mi arbolito 2", 30);
        list.addAll(Arrays.asList(tr1, tr2, tr3, tr4, tr5));

        // Get the source for the job
        final KeyValueSource<String, TreeRecord> source = KeyValueSource.fromList(list);

        // Creating the job with the source
        Job<String, TreeRecord> job = jobTracker.newJob(source);

        // Setting up the job
        ICompletableFuture<Map<String, Double>> futureJob = job
                .mapper(new TreeDiameterMapper())
                .combiner(new TreeDiameterCombinerFactory())
                .reducer(new TreeDiameterReducerFactory())
                .submit();

        // Wait and retrieve the result
        Map<String, Double> result = futureJob.get();

        // TreeSet to get ordered results, first descending by diameter, then by alphabetic name
        TreeSet<Map.Entry<String, Double>> orderedResults = new TreeSet<>((o1, o2) -> {
            int res = o2.getValue().compareTo(o1.getValue());
            return res == 0 ? o1.getKey().compareTo(o2.getKey()) : res;
        });
        orderedResults.addAll(result.entrySet());

        // Keeping the first n results, we use an iterator for this
        List<Map.Entry<String, Double>> firstNResults = new ArrayList<>(n);
        Iterator<Map.Entry<String, Double>> iterator = orderedResults.iterator();
        int count = 0;
        while (count < n && iterator.hasNext()){
            firstNResults.add(iterator.next());
            count++;
        }

        LOG.info("THE RESULT IS {}", firstNResults);
    }
}
