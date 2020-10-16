package ar.edu.itba.pod.client.exceptions;

/**
 * Exception to be thrown in case invalid program arguments are given
 */
public class InvalidArgumentsException extends Exception {
    public InvalidArgumentsException(String s){
        super(s);
    }
}
