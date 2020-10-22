package api.collators;

import com.hazelcast.mapreduce.Collator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class TreeDiameterCollator implements Collator<Map.Entry<String, Double>, List<Map.Entry<String, Double>>> {

    private final int n;

    // Comparator used to compare the Map.Entry objects with the Map-Reduce results
    // First comparison is made by value, in descending order
    // Second comparison is in alphabetic order
    private static final Comparator<Map.Entry<String, Double>> ENTRY_COMPARATOR = (o1, o2) -> {
        int res = o2.getValue().compareTo(o1.getValue());
        return res == 0 ? o1.getKey().compareTo(o2.getKey()) : res;
    };

    public TreeDiameterCollator(int n){
        this.n = n;
    }

    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Double>> iterable) {

        // TreeSet to get ordered results, first descending by diameter, then by alphabetic name
        TreeSet<Map.Entry<String, Double>> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);
        iterable.forEach(e -> {
            e.setValue(round(e.getValue(), 2)); // we want to round before we sort
            orderedResults.add(e);
        });

        // Keeping the first n results, we use an iterator for this
        List<Map.Entry<String, Double>> firstNResults = new ArrayList<>(this.n);
        Iterator<Map.Entry<String, Double>> iterator = orderedResults.iterator();
        int count = 0;
        while (count < n && iterator.hasNext()){
            firstNResults.add(iterator.next());
            count++;
        }

        return firstNResults;
    }

    /**
     *
     * @param val originl double value
     * @param n how many decimals we want to keep
     * @return double with n decimals
     */
    private double round(double val, int n) {
        if(n < 0) throw new IllegalArgumentException();

        BigDecimal bigDecimal = new BigDecimal(val);
        bigDecimal = bigDecimal.setScale(n, RoundingMode.HALF_UP);
        return bigDecimal.doubleValue();
    }
}
