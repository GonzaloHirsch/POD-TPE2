package ar.edu.itba.pod.client;

import api.TreeRecord;
import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;
import ar.edu.itba.pod.client.queries.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

public class Client {
    // CLUSTER CONSTANTS
    private static final String NAME = "g2";
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

            // Parsing the input files and populating the hazelcast structures
            ParseAndPopulateStructures(hz, arguments.getCity(), arguments.getInPath(), arguments.getQuery(), arguments.getOutPath());

            // TODO: TODA LA LOGICA VA ACA
            // TODO: AGREGAR CODIGO POR QUERY
            // TODO: SE PUEDEN HACER FUNCIONES QUE RECIBAN LOS PARAMETROS DE ARGUMENTS Y EL CLIENTE DE HAZELCAST
            // Query to be executed
            Optional<GenericQuery<?, ?>> optionalQuery = Optional.empty();
            Queries chosenQuery = arguments.getQuery();
            switch (chosenQuery) {
                case QUERY_1:
                    optionalQuery = Optional.of(new Query1(hz, arguments.getOutPath(), arguments.getCity()));
                    break;
                case QUERY_2:
                    optionalQuery = Optional.of(new Query2(hz, arguments.getOutPath(), arguments.getCity(), arguments.getMin()));
                    break;
                case QUERY_3:
                    optionalQuery = Optional.of(new Query3(hz, arguments.getOutPath(), arguments.getCity(), arguments.getN()));
                    break;
                case QUERY_4:
                    optionalQuery = Optional.of(new Query4(hz, arguments.getOutPath(), arguments.getCity(), arguments.getSpeciesName(), arguments.getMin()));
                    break;
                case QUERY_5:
                    optionalQuery = Optional.of(new Query5(hz, arguments.getOutPath(), arguments.getCity()));
                    break;
            }

            // Executing the query
            optionalQuery.orElseThrow(() -> new IllegalStateException("No query to perform")).executeQuery();
        } catch (IOException e) {
            System.out.println("ERROR: There was a problem while parsing files");
        } catch (IllegalStateException e) {
            System.out.println("No query chosen to be performed");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ERROR: Exception in the server");
        }

        // Exit the program, we need this because it keeps the hazelcast connection open otherwise
        System.exit(0);
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

    /**
     * Parses the input files and populates the hazelcast structures with the parsed information
     *
     * @param hz      Instance of the hazelcast client
     * @param city    City chosen to be analyzed
     * @param inPath  Path to the folder containing the input files
     * @param query   Query to be performed
     * @param outPath Path to the file for output
     * @throws IOException if there is an error parsing the files
     */
    private static void ParseAndPopulateStructures(HazelcastInstance hz, Cities city, String inPath, Queries query, String outPath) throws IOException {
        // Logging start time of parsing
        CustomLogger.GetInstance().writeTimestamp(
                new File(outPath).getParent() + "/" + query.get_logFilename(),
                "Inicio de la lectura del archivo",
                false
        );

        // File parsing to get both neighbours and tree records information
        Parser parser = new Parser(city, inPath);

        // Populating the structures depending on the query chosen
        switch (query) {
            case QUERY_1:
            case QUERY_2:
            case QUERY_3:
            case QUERY_4:
                parser.parse(false, true);
                FillList(hz, parser, city);
                break;
            case QUERY_5:
                parser.parse(true, true);
                FillList(hz, parser, city);
                FillMap(hz, parser, city);
                break;
        }

        // Logging end time of parsing
        CustomLogger.GetInstance().writeTimestamp(
                new File(outPath).getParent() + "/" + query.get_logFilename(),
                "Fin de lectura del archivo",
                true
        );
    }

    /**
     * Populates the List form Hazelcast using the data from the parser
     *
     * @param hz     Hazelcast Client instance
     * @param parser Parser with parsed data
     * @param city   City chosen
     */
    private static void FillList(HazelcastInstance hz, Parser parser, Cities city) {
        // Getting the structure from hazelcast
        final IList<TreeRecord> treeRecordList = hz.getList(Constants.TREE_RECORD_LIST + city.getValue());
        // Clearing the collection just in case
        treeRecordList.clear();
        // Populating the tree records
        treeRecordList.addAll(parser.getTreeRecords());
    }

    /**
     * Populates the Map form Hazelcast using the data from the parser
     *
     * @param hz     Hazelcast Client instance
     * @param parser Parser with parsed data
     * @param city   City chosen
     */
    private static void FillMap(HazelcastInstance hz, Parser parser, Cities city) {
        // Getting the structure from hazelcast
        final IMap<String, Long> neighbourhoodsMap = hz.getMap(Constants.NEIGHBOURHOOD_TREE_COUNT_MAP + city.getValue());
        // Clearing the collection just in case
        neighbourhoodsMap.clear();
        // Populating the neighbourhoods
        neighbourhoodsMap.putAll(parser.getNeighbours());
    }
}
