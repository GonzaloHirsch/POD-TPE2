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

    private final static char delimiter = ';';

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
        objectDataOutput.writeUTF(neighbourhoodName);
        objectDataOutput.writeUTF(street);
        objectDataOutput.writeUTF(commonName);
        objectDataOutput.writeDouble(diameter);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        neighbourhoodName = objectDataInput.readUTF();
        street = objectDataInput.readUTF();
        commonName = objectDataInput.readUTF();
        diameter = objectDataInput.readDouble();
    }
}
