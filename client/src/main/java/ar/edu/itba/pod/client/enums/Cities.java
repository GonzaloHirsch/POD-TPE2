package ar.edu.itba.pod.client.enums;

import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;

import java.util.Optional;

/**
 * Enum for supported cities in the program
 */
public enum Cities {
    BUE("BUE"), VAN("VAN");

    private final String value;

    Cities(String s){
        this.value = s;
    }

    public String getValue() {
        return value;
    }

    public static Cities FromValue(String s) throws InvalidArgumentsException {
        s = Optional.ofNullable(s).orElseThrow(() -> new InvalidArgumentsException("Invalid argument for City")).toUpperCase();
        for (Cities c : Cities.values()){
            if (c.value.equals(s)){
                return c;
            }
        }
        throw new InvalidArgumentsException("Invalid argument for City");
    }
}
