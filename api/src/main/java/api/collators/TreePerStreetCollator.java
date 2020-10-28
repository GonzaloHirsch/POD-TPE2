package api.collators;

import com.hazelcast.mapreduce.Collator;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.*;

public class TreePerStreetCollator implements
        Collator<Map.Entry<String, MutablePair<String, Long>>, List<Map.Entry<String, MutablePair<String, Long>>>> {

    // Comparator to sort results: in this case, entries are sorted by keys by alphabetical orders
    private static final Comparator<Map.Entry<String, MutablePair<String, Long>>> ENTRY_COMPARATOR =
            Map.Entry.comparingByKey();

    private final Map<String, Long> neighbourhoods;
    private final long min;

    public TreePerStreetCollator(Map<String, Long> neighbourhoods, long min) {
        this.neighbourhoods = neighbourhoods;
        this.min = min;
    }

    @Override
    public List<Map.Entry<String, MutablePair<String, Long>>> collate(
            Iterable<Map.Entry<String, MutablePair<String, Long>>> iterable) {
        // order set by entry_comparator
        TreeSet<Map.Entry<String, MutablePair<String, Long>>> orderedResults = new TreeSet<>(ENTRY_COMPARATOR);

        // adds results to this collection
        iterable.forEach(e -> {
            if(this.neighbourhoods.containsKey(e.getKey()) && e.getValue().right >= this.min) orderedResults.add(e);
        });

        // return results in a list
        return new ArrayList<>(orderedResults);
    }
}
