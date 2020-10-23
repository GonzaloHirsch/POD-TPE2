package api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query4Collator implements Collator<Map.Entry<String, Integer>, List<Map.Entry<String, List<String>>>> {
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
    public List<Map.Entry<String, List<String>>> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        // order set by entry_comparator
        TreeSet<String> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);

        // adds results to this collection
        //iterable.forEach(orderedResults::add);
        iterable.forEach(e -> {
            if(e.getValue() >= this.min) orderedResults.add(e.getKey());
        });

        List<String> results = new ArrayList<>(orderedResults);
        Map<String, List<String>> map = new HashMap<>();

        for (int i = 0; i < results.size()-1; i++) {
            map.put(results.get(i), new ArrayList<>());
            for (int j = i+1; j < results.size(); j++) {
                map.get(results.get(i)).add(results.get(j));
            }
        }

        // return results in a list
        return new ArrayList<>(map.entrySet());
    }
}