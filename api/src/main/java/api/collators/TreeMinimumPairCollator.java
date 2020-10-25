package api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class TreeMinimumPairCollator implements Collator<Map.Entry<String, Integer>, List<Map.Entry<String, List<String>>>> {
    private final int min;

    public TreeMinimumPairCollator(int min) {
        this.min = min;
    }

    private static final Comparator<String> ENTRY_COMPARATOR = String::compareTo;

    @Override
    public List<Map.Entry<String, List<String>>> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        // Order set by entry_comparator
        TreeSet<String> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);

        // Add keys that have more than the MIN number of trees
        iterable.forEach(e -> {
            if (e.getValue() >= this.min) {
                orderedResults.add(e.getKey());
            }
        });

        // Creating a sorted list
        List<String> results = new ArrayList<>(orderedResults);

        List<String> currList;
        List<Map.Entry<String, List<String>>> resultList = new ArrayList<>();
        for (int i = 0; i < results.size() - 1; i++) {
            currList = new ArrayList<>();
            for (int j = i + 1; j < results.size(); j++) {
                currList.add(results.get(j));
            }
            resultList.add(new AbstractMap.SimpleEntry<>(results.get(i), currList));
        }

        // return results in a list
        return resultList;
    }
}