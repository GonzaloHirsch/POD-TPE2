package ar.edu.itba.pod.client;

/**
 * Constants to be shared with query classes
 */
public final class Constants {
    private Constants () throws IllegalAccessException {
        throw new IllegalAccessException("Constants class cannot be instantiated");
    }

    // STRUCTURES CONSTANTS
    public static final String TREE_RECORD_LIST = "TREE_LIST_";
    public static final String NEIGHBOURHOOD_TREE_COUNT_MAP = "NEIGH_TREE_MAP_";
}
