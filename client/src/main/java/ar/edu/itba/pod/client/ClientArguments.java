package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.enums.Cities;
import ar.edu.itba.pod.client.enums.Queries;
import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class ClientArguments {
    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                      VARIABLES & KEYS
    ///////////////////////////////////////////////////////////////////////////////////////////////
    private Cities city;
    private Collection<String> addresses;
    private String inPath;
    private String outPath;
    private int n;
    private int min;
    private String speciesName;
    private Queries query;

    private static final String CITY_KEY = "city";
    private static final String ADDRESSES_KEY = "addresses";
    private static final String IN_KEY = "inPath";
    private static final String OUT_KEY = "outPath";
    private static final String MIN_KEY = "min";
    private static final String N_KEY = "n";
    private static final String NAME_KEY = "name";
    private static final String QUERY_KEY = "query";

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                          METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public void parseArguments() throws InvalidArgumentsException {
        Properties props = System.getProperties();

        // ----------------------------------------------------------------------
        // REQUIRED PARAMETERS
        // ----------------------------------------------------------------------

        // Try to obtain the CITY parameter
        if (!props.containsKey(CITY_KEY)) {
            this.printHelp();
            throw new InvalidArgumentsException("Invalid argument for City");
        } else {
            this.city = Cities.FromValue(props.getProperty(CITY_KEY));
        }

        // Try to obtain the IN parameter
        if (!props.containsKey(IN_KEY)) {
            this.printHelp();
            throw new InvalidArgumentsException("Invalid argument for In");
        } else {
            this.inPath = props.getProperty(IN_KEY);
        }

        // Try to obtain the OUT parameter
        if (!props.containsKey(OUT_KEY)) {
            this.printHelp();
            throw new InvalidArgumentsException("Invalid argument for Out");
        } else {
            this.outPath = props.getProperty(OUT_KEY);
        }

        // Try to obtain the ADDRESSES parameter
        if (!props.containsKey(ADDRESSES_KEY)) {
            this.printHelp();
            throw new InvalidArgumentsException("Invalid argument for Addresses");
        } else {
            this.addresses = Arrays.asList(props.getProperty(ADDRESSES_KEY).split(";"));
        }

        // Try to obtain the QUERY parameter
        if (!props.containsKey(QUERY_KEY)) {
            this.printHelp();
            throw new InvalidArgumentsException("Invalid argument for Query");
        } else {
            this.query = Queries.FromValue(Integer.parseInt(props.getProperty(QUERY_KEY)));
        }

        // ----------------------------------------------------------------------
        // OPTIONAL PARAMETERS
        // ----------------------------------------------------------------------

        // Try to obtain the N parameter
        if (this.query == Queries.QUERY_3) {
            if (!props.containsKey(N_KEY)) {
                this.printHelp();
                throw new InvalidArgumentsException("Invalid argument for N");
            } else {
                this.n = Integer.parseInt(props.getProperty(N_KEY));
                if (this.n <= 0){
                    this.printHelp();
                    throw new InvalidArgumentsException("Invalid argument for N");
                }
            }
        }

        // Try to obtain the MIN parameter
        if (this.query == Queries.QUERY_2 || this.query == Queries.QUERY_4){
            if (!props.containsKey(MIN_KEY)) {
                this.printHelp();
                throw new InvalidArgumentsException("Invalid argument for Min");
            } else {
                this.min = Integer.parseInt(props.getProperty(MIN_KEY));
            }
        }

        // Try to obtain the NAME parameter
        if (this.query == Queries.QUERY_4){
            if (!props.containsKey(NAME_KEY)) {
                this.printHelp();
                throw new InvalidArgumentsException("Invalid argument for Name");
            } else {
                this.speciesName = props.getProperty(NAME_KEY);
            }
        }
    }

    /**
     * Method to print the help for the management client
     */
    private void printHelp() {
        System.out.println("This program should be run as follows:\n" +
                "$>./queryX -Dcity=[BUE|VAN] -Daddresses='xx.xx.xx.xx:yyyy;xx.xx.xx.xx:yyyy' -DinPath=. -DoutPath=. -Dn=5 -Dmin=100 -Dname='Fraxinus pennsylvanica'\n" +
                "Where: \n" +
                " - Dcity is the city to be used, BUE or VAN \n" +
                " - Daddresses is xx.xx.xx.xx:yyyy with xx.xx.xx.xx is the server address and yyyy the port of the server, there can be multiple addresses and ports sepparated by ;\n" +
                " - DinPath is the path to the directory where input files are\n" +
                " - DoutPath is the path to the output file to be used\n" +
                " - Dn is the maximum number of results to show (QUERY 3)\n" +
                " - Dmin is the minimum amount to take into account (QUERY 4 and QUERY 2)\n" +
                " - Dname is the species name to filter by (QUERY 4)\n");
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                          GETTERS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public Cities getCity() {
        return city;
    }

    public Collection<String> getAddresses() {
        return addresses;
    }

    public String getInPath() {
        return inPath;
    }

    public String getOutPath() {
        return outPath;
    }

    public int getN() {
        return n;
    }

    public int getMin() {
        return min;
    }

    public String getSpeciesName() {
        return speciesName;
    }

    public Queries getQuery() {
        return query;
    }
}

