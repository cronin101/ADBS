package org.dejave.attica.storage;

import org.dejave.attica.storage.Tuple;

import java.util.Comparator;

/**
 * TupleComparator: A Comparator that will handle ordering
 * of tuples by the 'slots' array handed to ExternalSort */

public class TupleComparator implements Comparator<Tuple> {
    private int [] slots;

    public TupleComparator(int [] slots) {
        this.slots = slots;
    }

    @Override
    public int compare(Tuple a, Tuple b) {
        for (int slot : slots ) {
            int ordering = a.getValue(slot).compareTo(b.getValue(slot));
            if (ordering != 0) return ordering;
        }
        return 0;
    }
}
