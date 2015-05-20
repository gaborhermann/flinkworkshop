package hu.sztaki.workshop.hadoop.day3.kmeans.distance;

import de.jungblut.math.DoubleVector;
import de.jungblut.math.DoubleVector.DoubleVectorElement;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Mapper that maps sparse vectors into a set of their indices so they can be
 * used in the {@link InvertedIndex} for fast lookup.
 *
 * @author thomas.jungblut
 *
 */
public final class SparseVectorDocumentMapper implements
        InvertedIndex.DocumentMapper<DoubleVector, Integer> {

    @Override
    public Set<Integer> mapDocument(DoubleVector v) {
        Set<Integer> set = new HashSet<>(v.getLength());
        Iterator<DoubleVectorElement> iterateNonZero = v.iterateNonZero();
        while (iterateNonZero.hasNext()) {
            DoubleVectorElement next = iterateNonZero.next();
            set.add(next.getIndex());
        }
        return set;
    }

}