package ar.edu.itba.pod.client.enums;

import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;

import java.util.Optional;

public enum Queries {
    QUERY_1(1), QUERY_2(2), QUERY_3(3), QUERY_4(4), QUERY_5(5);

    private final int _id;

    Queries(int id){
        this._id = id;
    }

    public static Queries FromValue(int id) throws InvalidArgumentsException {
        for (Queries q : Queries.values()){
            if (q._id == id){
                return q;
            }
        }
        throw new InvalidArgumentsException("Invalid argument for Query");
    }
}
