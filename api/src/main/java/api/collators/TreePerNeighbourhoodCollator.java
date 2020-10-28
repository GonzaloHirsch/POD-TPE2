package api.collators;

import com.hazelcast.mapreduce.Collator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class TreePerNeighbourhoodCollator implements
        Collator<Map.Entry<String, Integer>, List<Map.Entry<String, Double>>> {

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // First comparison is made by value, in descending order
    // Second comparison is in alphabetic order
    private static final Comparator<Map.Entry<String, Double>> ENTRY_COMPARATOR = (o1, o2) -> {
        int res = o2.getValue().compareTo(o1.getValue());
        return res == 0 ? o1.getKey().compareTo(o2.getKey()) : res;
    };

    private final Map<String, Long> neighbourhoods;

    public TreePerNeighbourhoodCollator(Map<String, Long> neighbourhoods) {
        this.neighbourhoods = neighbourhoods;
    }

    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        // order set by entry_comparator
        TreeSet<Map.Entry<String, Double>> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);

        Map<String, Double> avgTreePerPerson = new HashMap<>();

        // adds results to this collection
        iterable.forEach(r -> {
            if (this.neighbourhoods.containsKey(r.getKey())) {
                double avg = round(((double) r.getValue()) / neighbourhoods.get(r.getKey()), 2);
                avgTreePerPerson.put(r.getKey(), avg);
            }
        });

        // Adds avg trees per person in order for records to be sorted
        orderedResults.addAll(avgTreePerPerson.entrySet());

        // return results in a list
        return new ArrayList<>(orderedResults);
    }

    /**
     * @param val originl double value
     * @param n   how many decimals we want to keep
     * @return double with n decimals
     */
    private double round(double val, int n) {
        if (n < 0) throw new IllegalArgumentException();

        BigDecimal bigDecimal = new BigDecimal(val);
        bigDecimal = bigDecimal.setScale(n, RoundingMode.HALF_UP);
        return bigDecimal.doubleValue();
    }
}
