package ar.edu.itba.pod.client;

import api.TreeRecord;
import ar.edu.itba.pod.client.enums.Cities;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class Parser {
    private final String neighboursPath;
    private final String treeRecordsPath;

    private final Map<String, Long> neighbourhoods = new HashMap<>();
    private final List<TreeRecord> treeRecords = new ArrayList<>();

    private final Set<String> neighbourhoodHeader = new HashSet<>(Arrays.asList("comuna", "neighbourhood_name"));
    private final Set<String> streetHeader = new HashSet<>(Arrays.asList("calle_nombre", "std_street"));
    private final Set<String> commonNameHeader = new HashSet<>(Arrays.asList("nombre_cientifico", "common_name"));
    private final Set<String> diameterHeader = new HashSet<>(Arrays.asList("diametro_altura_pecho", "diameter"));

    private int NEIGHBOURHOOD_NAME_ID;
    private int STREET_ID;
    private int COMMON_NAME_ID;
    private int DIAMETER_ID;

    public Parser(Cities city, String inPath) {
        String EXTENSION = ".csv";
        this.neighboursPath = inPath.concat("barrios").concat(city.name()).concat(EXTENSION);
        this.treeRecordsPath = inPath.concat("arboles").concat(city.name()).concat(EXTENSION);
    }

    /**
    Once the parser is initialized, we should call this method in order to
    parse both neighbours file and tree records file. We can access to the
    information through getters for both neighbours mao and tree records list.
    */
    public void parse() throws IOException {
        parseNeighbourhoods();
        parseTreeRecords();
    }

    public Map<String, Long> getNeighbours() {
        return neighbourhoods;
    }

    public List<TreeRecord> getTreeRecords() {
        return treeRecords;
    }

    /**
    Parses neighbours file and stores information in neighbours map
     */
    private void parseNeighbourhoods() throws IOException {
        List<String> lines = Files.readAllLines(new File(this.neighboursPath).toPath());
        lines.remove(0); // We do not need headers

        String[] neighbourParts;

        for (String line : lines) { // For each line...
            neighbourParts = line.trim().split(";");

            // We put neighbour with population in our neighbours map
            int POPULATION_ID = 1;
            int NAME_ID = 0;
            neighbourhoods.put(neighbourParts[NAME_ID], Long.valueOf(neighbourParts[POPULATION_ID]));
        }
    }

    /**
    Parses tree records file and stores information in tree records list
    */
    private void parseTreeRecords() throws IOException {
        List<String> lines = Files.readAllLines(new File(this.treeRecordsPath).toPath(), StandardCharsets.ISO_8859_1);
        String[] headers;

        Iterator<String> iterator = lines.iterator();
        String headersLine = iterator.next();
        headers = headersLine.toLowerCase().trim().split(";");
        setTreeRecordsIndexes(Arrays.asList(headers));

        String[] treeRecordElements;

        while (iterator.hasNext()) {
            treeRecordElements = iterator.next().trim().split(";");
            treeRecords.add(new TreeRecord(
                    treeRecordElements[NEIGHBOURHOOD_NAME_ID], treeRecordElements[STREET_ID],
                    treeRecordElements[COMMON_NAME_ID], Double.parseDouble(treeRecordElements[DIAMETER_ID])
            ));
        }
    }

    private void setTreeRecordsIndexes(List<String> headers) {
        Iterator<String> iterator = headers.iterator();
        String header;
        int search = 0;
        while (iterator.hasNext() && search < 4) {
            header = iterator.next();
            search = findHeader(search, header, headers);
        }
    }

    /**
     * This method is necessary since needed headers are not always at the same indexes
     */
    private int findHeader(int search, String header, List<String> headers) {
        switch (search) {
            case 0: { // case of neighbourhood
                if (neighbourhoodHeader.contains(header)) {
                    NEIGHBOURHOOD_NAME_ID = headers.indexOf(header);
                    return search + 1;
                }
            }
            case 1: { // case of street
                if (streetHeader.contains(header)) {
                    STREET_ID = headers.indexOf(header);
                    return search + 1;
                }
            }
            case 2: { // case of common name
                if (commonNameHeader.contains(header)) {
                    COMMON_NAME_ID = headers.indexOf(header);
                    return search + 1;
                }
            }
            case 3: { // case of diameter
                if (diameterHeader.contains(header)) {
                    DIAMETER_ID = headers.indexOf(header);
                    return search + 1;
                }
            }
            default:
                return search;
        }
    }
}
