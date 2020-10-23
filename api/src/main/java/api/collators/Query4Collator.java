package api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;

public class Query4Collator implements Collator<Map.Entry<String, Integer>, List<Map.Entry<String, String>>> {
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
    public List<Map.Entry<String, String>> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        // order set by entry_comparator
        TreeSet<String> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);

        // adds results to this collection
        //iterable.forEach(orderedResults::add);
        iterable.forEach(e -> {
            if(e.getValue() >= this.min) orderedResults.add(e.getKey());
        });

        List<String> results = new ArrayList<>(orderedResults);
        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < results.size()-1; i++) {
            for (int j = i; j < results.size(); j++) {
                map.put(results.get(i), results.get(j));
            }
        }

        // return results in a list
        return new ArrayList<>(map.entrySet());
    }
}