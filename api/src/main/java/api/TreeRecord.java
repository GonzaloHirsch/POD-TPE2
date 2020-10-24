package api;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class TreeRecord implements DataSerializable {

    private String neighbourhoodName;
    private String street;
    private String commonName;
    private double diameter;

    private final static String delimiter = ";";

    public TreeRecord() {}

    public TreeRecord(String neighbourhoodName, String street, String commonName, double diameter) {
        this.neighbourhoodName = neighbourhoodName;
        this.street = street;
        this.commonName = commonName;
        this.diameter = diameter;
    }

    public String getNeighbourhoodName() {
        return neighbourhoodName;
    }

    public String getStreet() {
        return street;
    }

    public String getCommonName() {
        return commonName;
    }

    public double getDiameter() {
        return diameter;
    }

    @Override
    public String toString() {
        return neighbourhoodName +
                delimiter +
                street +
                delimiter +
                commonName +
                delimiter +
                diameter;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeObject(this.toString() + "\n");
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        String[] recordElements = ((String)objectDataInput.readObject()).split(delimiter);
        neighbourhoodName = recordElements[0];
        street = recordElements[1];
        commonName = recordElements[2];
        diameter = Double.valueOf(recordElements[3]);
    }
}
