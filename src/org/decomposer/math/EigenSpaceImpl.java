package org.decomposer.math;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.IntDoublePairImpl;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.array.DenseMapVectorFactory;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.util.FixedSizeSortedSet;

/**
 * @author jmannix
 */
public class EigenSpaceImpl implements EigenSpace
{
  protected final DoubleMatrix _eigenvectors;
  protected final double[] _singularValues;
  protected final double[] _inverseSingularValues;
  
  public EigenSpaceImpl(DoubleMatrix eigenvectors, Double[] singularValues)
  {
    _eigenvectors = eigenvectors;
    _singularValues = new double[singularValues.length];
    _inverseSingularValues = new double[singularValues.length];
    for(int i=0; i<singularValues.length; i++)
    {
      _singularValues[i] = singularValues[i] > 0 ? Math.sqrt(singularValues[i]) : 0;
      _inverseSingularValues[i] = _singularValues[i] > 0 ? (1 / _singularValues[i]) : 0;
    }
  }
  
  public MapVector project(MapVector documentVector)
  {
    MapVector output = _eigenvectors.times(documentVector);
    for(IntDoublePair pair : output)
    {
      if(pair.getInt() >= _inverseSingularValues.length) break;
      pair.setDouble(pair.getDouble() * _inverseSingularValues[pair.getInt()]);
    }
    return output;
  }
  
  public MapVector uplift(MapVector projectedVector)
  {
    MapVector output = new DenseMapVectorFactory().zeroVector(_eigenvectors.numCols());
    for(Map.Entry<Integer, MapVector> eigenVector : _eigenvectors)
    {
      output.plus(eigenVector.getValue(), projectedVector.get(eigenVector.getKey()) * _singularValues[eigenVector.getKey()]);
    }
    return output;
  }
  
  public MapVector expand(MapVector documentVector, int numOutpuNonZeroEntries)
  {
    return sparsify(uplift(project(documentVector)), numOutpuNonZeroEntries);
  }
  
  public MapVector sparsify(MapVector input, final int numNonZeroEntries)
  {
    Comparator<IntDoublePair> comparator = new Comparator<IntDoublePair>()
    {
      public int compare(IntDoublePair o1, IntDoublePair o2)
      {
        return Math.abs(o1.getDouble()) - Math.abs(o2.getDouble()) > 0 ? 1 : -1;
      }
    };
    Set<IntDoublePair> bestPairs = new FixedSizeSortedSet<IntDoublePair>(comparator, numNonZeroEntries);
    for(final IntDoublePair pair : input) bestPairs.add(new IntDoublePairImpl(pair));
    List<IntDoublePair> resortedPairs = new ArrayList<IntDoublePair>(bestPairs);
    Collections.sort(resortedPairs, new Comparator<IntDoublePair>()
                     {
                        public int compare(IntDoublePair o1, IntDoublePair o2)
                        {
                          if(o1.getInt() == o2.getInt()) return 0;
                          return o1.getInt() - o2.getInt() > 0 ? 1 : -1;
                        }
                     });
    int[] indices = new int[numNonZeroEntries];
    double[] values = new double[numNonZeroEntries];
    int i = 0;
    for(IntDoublePair pair : resortedPairs)
    {
      indices[i] = pair.getInt();
      values[i] = pair.getDouble();
      i++;
      if(i >= numNonZeroEntries) break;
    }
    return new ImmutableSparseMapVector(indices, values);
  }
}
