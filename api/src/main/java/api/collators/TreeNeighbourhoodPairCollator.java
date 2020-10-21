package api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class TreeNeighbourhoodPairCollator
        implements Collator<Map.Entry<Long, TreeSet<String>>, List<Map.Entry<Long, TreeSet<String>>>> {

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // First comparison is made by value, in descending order
    // Second comparison is in alphabetic order
    private static final Comparator<Map.Entry<Long, TreeSet<String>>> ENTRY_COMPARATOR =
            (o1, o2) -> o2.getKey().compareTo(o1.getKey());

    @Override
    public List<Map.Entry<Long, TreeSet<String>>> collate(Iterable<Map.Entry<Long, TreeSet<String>>> iterable) {
        // order set by entry_comparator
        TreeSet<Map.Entry<Long, TreeSet<String>>> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);

        // adds results to this collection
        iterable.forEach(orderedResults::add);

        // return results in a list
        return new ArrayList<>(orderedResults);
    }
}
