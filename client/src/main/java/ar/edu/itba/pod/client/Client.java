package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.enums.Queries;
import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;

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

            // TODO: PARSEAR ARCHIVOS DE ENTRADA ACA
            // TODO: USAR LOS PARAMETROS in Y cities PARA HACER PARSEOS


            // TODO: TODA LA LOGICA VA ACA
            // TODO: AGREGAR CODIGO POR QUERY
            // TODO: SE PUEDEN HACER FUNCIONES QUE RECIBAN LOS PARAMETROS DE ARGUMENTS Y EL CLIENTE DE HAZELCAST
            Queries chosenQuery = arguments.getQuery();
            switch (chosenQuery){
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
        } catch (Exception e) {
            // FIXME: BETTER ERRORS HERE
            System.out.println("ERROR: Exception in the server");
        }
    }

    /**
     * Creates an instance of the Hazelcast Client based on a collection of ips + ports for the members.
     * <br><br>
     * Group config and network config example from: https://www.codota.com/code/java/classes/com.hazelcast.client.config.ClientConfig
     * @param memberIPs Collection of IP:PORT strings for each member
     * @return an instance of a Hazelcast Client
     */
    private static HazelcastInstance GetHazelInstance(Collection<String> memberIPs){
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
