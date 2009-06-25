package org.decomposer.contrib.lsh;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import junit.framework.TestCase;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.DenseMapVectorFactory;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;

import org.decomposer.util.FixedSizeSortedSet;

/**
 *
 * @author jmannix
 */
public class LocalitySensitiveHashIndexImplTest extends TestCase
{

  private VectorFactory _vectorFactory;
  private LocalitySensitiveHashImpl _lsh;
  private int numBits;
  private int numPermutations;
  private int sourceDimension;
  private int numResultsDesired;
  private boolean sparse;
  
  public LocalitySensitiveHashIndexImplTest(String testName)
  {
    super(testName);
  }

  @Override
  protected void setUp() throws Exception
  {
    super.setUp();
    _vectorFactory = new DenseMapVectorFactory(); 
    numBits = 20;
    numPermutations = 100;
    sourceDimension = (int) 1e4;
    sparse = false;
    numResultsDesired = 10;
  }

  @Override
  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  /**
   * Test of add method, of class LocalitySensitiveHashIndexImpl.
   */
  public void testAdd()
  {
    LocalitySensitiveHashIndexImpl instance = buildLshIndex();
    MapVector docVector = randomVector();
    
    instance.add(docVector);
    instance.sortAndSeal();
    MapVector nearest = instance.findNearest(docVector).get(0);
    assertTrue(isWithinEpsilon(1.0d, docVector.dot(nearest) / (docVector.norm() * nearest.norm()), 1e-9));
  }

  private boolean isWithinEpsilon(double d, double e, double f)
  {
    return Math.abs(d - e) < f;
  }

  /**
   * Test of sortAndSeal method, of class LocalitySensitiveHashIndexImpl.
   */
  public void testSortAndSeal()
  {
    LocalitySensitiveHashIndexImpl instance = buildLshIndex();
    instance.sortAndSeal();
    try
    {
      instance.add(randomVector());
      fail("Should have thrown IllegalStateException: is sealed");
    }
    catch(IllegalStateException ise)
    {
      // expected
    }
    
    instance = buildLshIndex();
    instance.add(randomVector());
    try
    {
      instance.findNearest(randomVector());
      fail("Should have thrown IllegalStateException: not yet sealed");
    }
    catch(IllegalStateException ise)
    {
      // expected
    }
  }

  /**
   * Test of findNearest method, of class LocalitySensitiveHashIndexImpl.
   */
  public void _testFindNearestMapVector()
  {
    int numClusters = 100;
    int clusterSize = 10;
    double epsilon = 0.0025;
    int numResults = 100;
    
    numBits = 48;
    numPermutations = 80;
    sourceDimension = (int) 100;
    sparse = false;
    numResultsDesired = 10;
    while(sourceDimension < 1000)
    {
      sourceDimension *= 10;
      int tmpPermutations = numPermutations;
      while(numPermutations < 100)
      {
        numPermutations *= 4;  

        int tmpNumBits = numBits;
        while(numBits <= 48)
        {
          numBits += 16;
          double tmpEpsilon = epsilon;
          while(epsilon < 0.01)
          {
            epsilon *= 4;
     //       System.out.println("Cluster angle radius: " + epsilon);
            double meanNumFound = 0;
            for(int queryI = 0; queryI < numClusters; queryI++)
            {
              DoubleMatrix testMatrix = new HashMapDoubleMatrix(_vectorFactory);

              for(int i=0; i<numClusters; i++)
              {
                List<MapVector> cluster = randomCluster(clusterSize, epsilon);
                for(int j=0; j<cluster.size(); j++)
                  testMatrix.set(j + clusterSize*i, cluster.get(j));
              }
              Map<MapVector, List<MapVector>> closest = findClosest(testMatrix, numResults);

              MapVector queryVector = testMatrix.get(queryI*clusterSize);
              List<MapVector> expResult = closest.get(queryVector);

              LocalitySensitiveHashIndexImpl instance = buildLshIndex();
              for(int i=0; i<testMatrix.numRows(); i++)
                instance.add(testMatrix.get(i));
              instance.sortAndSeal();

              List<MapVector> result = instance.findNearest(queryVector, numResults);

              Set<MapVector> expectedSet = new HashSet<MapVector>();
              expectedSet.addAll(expResult);
              int found = 0;
              for(MapVector v : result)
              {
                if(expectedSet.contains(v)) found++;
              }
              meanNumFound += found;
             // System.out.println("Found " + found + " out of " + expectedSet.size() + " in cluster " + queryI);
           //   assertTrue(found > 0);
            }
            System.out.println("dim_source: " + sourceDimension + ", with " + numPermutations 
              + " permutations of " + numBits + " bits per hash, found " 
              + (double)(meanNumFound / numClusters) + " out of " + numResults + " clustered within " + epsilon);  
          }
          epsilon = tmpEpsilon;
        }  
        numBits = tmpNumBits;
      }
      numPermutations = tmpPermutations;
    }
  }

  private LocalitySensitiveHashIndexImpl buildLshIndex()
  {
    _lsh = new LocalitySensitiveHashImpl(numBits, numPermutations, sourceDimension, sparse);
   
    LocalitySensitiveHashIndexImpl instance = new LocalitySensitiveHashIndexImpl(_lsh, 
                                                                                 numPermutations, 
                                                                                 numResultsDesired);
    return instance;
  }
  
  private final MapVector randomVector()
  {
    return randomVector(_vectorFactory, sourceDimension, sourceDimension);
  }
  
  public static final MapVector randomVector(VectorFactory vectorFactory, int numNonZero, int sourceDimension)
  {
    MapVector v;
    Random r = new Random();
    if(numNonZero == sourceDimension)
    {
      v = vectorFactory.zeroVector(sourceDimension);
      for(int i=0; i<sourceDimension; i++) v.set(i, r.nextGaussian());
    }
    else
    {
      v = new HashMapVectorFactory().zeroVector(numNonZero);
      for(int i=0; i<numNonZero; i++) v.set(r.nextInt(sourceDimension), 2 * ((double)r.nextInt(2) - 0.5));
      v = new ImmutableSparseMapVector(v);
    }
    return v;
  }
  
  private final List<MapVector> randomCluster(int clusterSize, double epsilon)
  {
    MapVector centroid = randomVector();
    centroid.scale(1/centroid.norm());
    List<MapVector> l = new ArrayList<MapVector>(clusterSize);
    l.add(centroid);
    for(int i=0; i<clusterSize - 1; i++)
    {
      MapVector w = randomVector();
      w.scale(1/w.norm());
      w.scale(Math.sqrt(2*epsilon));
      MapVector newVector = _vectorFactory.zeroVector().plus(centroid).plus(w);
      l.add(newVector);
    }
    return l;
  }
  
  private Map<MapVector, List<MapVector>> findClosest(DoubleMatrix matrix, int numClosest)
  {
    Map<MapVector, List<MapVector>> results = new HashMap<MapVector, List<MapVector>>();
    for(int i=0; i<matrix.numRows(); i++)
    {
      Set<VectorResult> entryResults 
        = new FixedSizeSortedSet<VectorResult>(numClosest);
      MapVector u = matrix.get(i);
      for(int j = 0; j<matrix.numRows(); j++)
      {
        MapVector v = matrix.get(j);
        double dot = -1;
        if(u.norm() > 0 && v.norm() > 0) dot = u.dot(v) / (u.norm() * v.norm());
        VectorResult res = new VectorResult();
        res._score = dot;
        res._v = v;
        entryResults.add(res);
      }
      List<MapVector> closest = new ArrayList<MapVector>(numClosest);
      for(VectorResult e : entryResults) closest.add(e._v);
      results.put(u, closest);
    }
    return results;
  }
  
  private static final class VectorResult implements Comparable<VectorResult>
  {
    Double _score;
    MapVector _v;
    public int compareTo(VectorResult o)
    {
      if(o._score == _score) return 0;
      return _score > o._score ? 1 : -1;
    }
  }

}
