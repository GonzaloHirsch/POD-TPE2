package api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query4Collator implements Collator<Map.Entry<String, Integer>, List<String>> {
    private int min;
    public Query4Collator(int min){
        this.min=min;
    }

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // First comparison is made by value, in descending order
    // Second comparison is in alphabetic order
    private static final Comparator<String> ENTRY_COMPARATOR =
        (o1, o2) -> String.valueOf(o2).compareTo(String.valueOf(o1));

    @Override
    public List<String> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        // order set by entry_comparator
        TreeSet<String> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);

        // adds results to this collection
        //iterable.forEach(orderedResults::add);
        iterable.forEach(e -> {
            if(e.getValue() >= this.min) orderedResults.add(e.getKey());
        });


        // return results in a list
        return new ArrayList<>(orderedResults);
    }
}