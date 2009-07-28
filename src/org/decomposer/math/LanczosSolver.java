package org.decomposer.math;


import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.linear.EigenDecomposition;
import org.apache.commons.math.linear.EigenDecompositionImpl;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.util.MathUtils;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.DenseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;

public class LanczosSolver implements TimingConstants
{
  
  public void solve(DoubleMatrix corpus, 
                    int desiredRank,
                    DoubleMatrix eigenVectors, 
                    List<Double> eigenValues)
  {
    VectorFactory vf = new HashMapVectorFactory();
    MapVector previousVector = vf.zeroVector();
    MapVector currentVector = getInitialVector(corpus);
    DoubleMatrix basis = new HashMapDoubleMatrix(vf);
    basis.set(0, currentVector);
    double alpha = 0;
    double beta = 0;
    DoubleMatrix triDiag = new HashMapDoubleMatrix(vf);
    triDiag.set(0, vf.zeroVector());
    for(int i=1; i<desiredRank; i++)
    {
      startTime(TimingSection.ITERATE);
      MapVector nextVector = corpus.timesSquared(currentVector);
      nextVector.plus(previousVector, -beta);
      // now orthogonalize
      alpha = currentVector.dot(nextVector);
      nextVector.plus(currentVector, -alpha);
      endTime(TimingSection.ITERATE);
      startTime(TimingSection.ORTHOGANLIZE);
      orthoganalizeAgainstAllButLast(nextVector, basis);
      endTime(TimingSection.ORTHOGANLIZE);
      // and normalize
      beta = nextVector.norm();
      nextVector.scale(1/beta);
      basis.set(i, nextVector);
      previousVector = currentVector;
      currentVector = nextVector;
      // save the projections and norms!
      triDiag.get(i-1).set(i-1, alpha);
      if(i < desiredRank - 1)
      {
        triDiag.get(i-1).set(i, beta);
        triDiag.set(i, vf.zeroVector());
        triDiag.get(i).set(i-1, beta);
      }
    }
    startTime(TimingSection.TRIDIAG_DECOMP);
    double[] diag = new double[triDiag.numRows()];
    for(int i=0; i<diag.length; i++) diag[i] = triDiag.get(i).get(i);
    double[] offDiag = new double[triDiag.numRows() - 1];
    for(int i=0; i<offDiag.length; i++) offDiag[i] = triDiag.get(i).get(i+1);
    // at this point, have tridiag all filled out, and basis is all filled out, and orthonormalized
    EigenDecomposition decomp = new EigenDecompositionImpl(diag, offDiag, MathUtils.SAFE_MIN);
    endTime(TimingSection.TRIDIAG_DECOMP);
    startTime(TimingSection.FINAL_EIGEN_CREATE);
    for(int i=0; i<basis.numRows()-1; i++)
    {
      RealVector vector = decomp.getEigenvector(i);
      MapVector realEigen = new DenseMapVector();
      for(int j=0; j<vector.getDimension(); j++)
        realEigen.plus(basis.get(j), vector.getEntry(j));
      eigenVectors.set(i, realEigen);
      eigenValues.add(decomp.getRealEigenvalue(i));
    }
    endTime(TimingSection.FINAL_EIGEN_CREATE);
  }

  
  
  private void orthoganalizeAgainstAllButLast(MapVector nextVector, DoubleMatrix basis)
  {
    for(int i=0; i < basis.numRows() - 1; i++)
    {
      double alpha = nextVector.dot(basis.get(i));
      nextVector.plus(basis.get(i), -alpha);
    }
  }

  private MapVector getInitialVector(DoubleMatrix corpus)
  {
    MapVector v;
    int i;
    for(i = 0, v = null; i < corpus.numRows() && v == null; i++) v = corpus.get(i);
    MapVector oldV = v;
    v = new DenseMapVector();
    for(IntDoublePair pair : oldV)
      v.set(pair.getInt(), pair.getDouble());
    for(Map.Entry<Integer, MapVector> entry : corpus)
    {
      if(entry.getKey() != i) v.plus(entry.getValue());
    }
    v.scale(1/v.norm());
    return v;
  }
  
  private void startTime(TimingSection section)
  {
    startTimes.put(section, System.nanoTime());
  }
  
  private void endTime(TimingSection section)
  {
    if(!times.containsKey(section)) times.put(section, 0L);
    times.put(section, times.get(section) + (System.nanoTime() - startTimes.get(section)));
  }
  
  public double getTimeMillis(TimingSection section)
  {
    return ((double)times.get(section))/NANOS_IN_MILLI;
  }
  
  public static enum TimingSection { ITERATE, ORTHOGANLIZE, TRIDIAG_DECOMP, FINAL_EIGEN_CREATE }

  private Map<TimingSection, Long> startTimes = new EnumMap<TimingSection, Long>(TimingSection.class);
  private Map<TimingSection, Long> times = new EnumMap<TimingSection, Long>(TimingSection.class);
}
