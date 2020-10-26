package ar.edu.itba.pod.client.enums;

import ar.edu.itba.pod.client.exceptions.InvalidArgumentsException;

import java.util.Optional;

public enum Queries {
    QUERY_1(1), QUERY_2(2), QUERY_3(3), QUERY_4(4), QUERY_5(5);

    private final int _id;
    private final String _logFilename;
    private final String _outFilename;

    public String get_logFilename() {
        return _logFilename;
    }
    public String get_outFilename() {
        return _outFilename;
    }

    Queries(int id){
        this._id = id;
        this._logFilename = "query" + this._id + ".txt";
        this._outFilename = "query" + this._id + ".csv";
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
