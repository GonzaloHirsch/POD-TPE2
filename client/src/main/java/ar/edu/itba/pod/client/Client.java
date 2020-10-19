package ar.edu.itba.pod.client;

import api.TreeRecord;
import ar.edu.itba.pod.client.enums.Queries;
import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;
import ar.edu.itba.pod.client.queries.Query;
import ar.edu.itba.pod.client.queries.Query3;
import ar.edu.itba.pod.client.queries.Query5;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Client {
    private static Logger LOG = LoggerFactory.getLogger(Client.class);

    // CLUSTER CONSTANTS
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
            // Query to be executed
            Optional<Query> optionalQuery = Optional.empty();
            Queries chosenQuery = arguments.getQuery();
            switch (chosenQuery) {
                case QUERY_1:
                    break;
                case QUERY_2:
                    break;
                case QUERY_3:
                    optionalQuery = Optional.of(new Query3(hz, arguments.getOutPath(), arguments.getCity(), arguments.getN()));
                    break;
                case QUERY_4:
                    break;
                case QUERY_5:
                    optionalQuery = Optional.of(new Query5(hz, arguments.getOutPath(), arguments.getCity()));
                    break;
            }

            // Executing the query
            optionalQuery.orElseThrow(() -> new IllegalStateException("No query to perform")).executeQuery();

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
}
