package api;

import java.io.Serializable;

public class TreeRecord implements Serializable {
    private static final long serialVersionUID = 1920965438822291259L;

    private final String neighbourhoodName;
    private final String street;
    private final String commonName;
    private final double diameter;

    private final static char delimiter = ';';

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
        StringBuilder builder = new StringBuilder();
        builder.append(neighbourhoodName);
        builder.append(delimiter);
        builder.append(street);
        builder.append(delimiter);
        builder.append(commonName);
        builder.append(delimiter);
        builder.append(diameter);
        return builder.toString();
    }
}
