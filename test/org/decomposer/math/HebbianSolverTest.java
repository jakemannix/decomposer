package org.decomposer.math;



import java.io.File;
import java.util.Map.Entry;

import org.decomposer.math.HebbianSolver;
import org.decomposer.math.HebbianUpdater;
import org.decomposer.math.MultiThreadedEigenVerifier;
import org.decomposer.math.SingularVectorVerifier;
import org.decomposer.math.TrainingState;
import org.decomposer.math.vector.DiskBufferedDoubleMatrix;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.DenseMapVectorFactory;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;
import org.decomposer.math.vector.hashmap.TestDoubleMatrix;

import junit.framework.TestCase;

public class HebbianSolverTest extends TestCase
{
  private HebbianSolver _solver;
  private VectorFactory _corpusProjectionsVectorFactory;
  private VectorFactory _eigensVectorFactory;
  
  public static final String TMP_EIGEN_DIR = "tmp.HebbianSolverTest.eigenDir";
  
  public HebbianSolverTest(String name)
  {
    super(name);
  }

  private long timeSolver(String saveDir, double convergence, int maxNumPasses)
  {
    long time = 0;
    HebbianUpdater updater = new HebbianUpdater();
    SingularVectorVerifier verifier = new MultiThreadedEigenVerifier();
    _solver = new HebbianSolver(updater, 
                                _corpusProjectionsVectorFactory,
                                _eigensVectorFactory, 
                                verifier, 
                                saveDir, 
                                convergence, 
                                maxNumPasses);
    DoubleMatrix corpus = TestDoubleMatrix.randomImmutableSparseDoubleMatrix(2000, 1900, 1600, 100, 1.0);
    int desiredRank = 10;
    long start = System.nanoTime();
    TrainingState finalState = _solver.solve(corpus, desiredRank);
    time += System.nanoTime() - start;
    assertEquals(finalState.currentEigens.numRows(), desiredRank);
    return (long)(time / 1e6);
  }
  
  public void testHebbianSolver() throws Exception
  {
    _corpusProjectionsVectorFactory = new DenseMapVectorFactory();
    _eigensVectorFactory = new DenseMapVectorFactory();
    
    long optimizedTime = timeSolver(null, 0.00001, 5);

    _corpusProjectionsVectorFactory = new HashMapVectorFactory();
    _eigensVectorFactory = new HashMapVectorFactory();
    
    long unoptimizedTime = timeSolver(null, 0.00001, 5);
    
    System.out.println("Avg solving (unoptimized) time in ms:" + unoptimizedTime);
    System.out.println("Avg solving   (optimized) time in ms:" + optimizedTime);    
  }
  
  public void testSolverWithSerialization() throws Exception
  {
    _corpusProjectionsVectorFactory = new DenseMapVectorFactory();
    _eigensVectorFactory = new DenseMapVectorFactory();
    
    timeSolver(TMP_EIGEN_DIR, 0.001, 5);
    
    File eigenDir = new File(TMP_EIGEN_DIR + File.separator + HebbianSolver.EIGEN_VECT_DIR);
    DiskBufferedDoubleMatrix eigens = new DiskBufferedDoubleMatrix(eigenDir, 10);
    
    DoubleMatrix inMemoryMatrix = new HashMapDoubleMatrix(_corpusProjectionsVectorFactory, eigens);
    
    for(Entry<Integer, MapVector> diskEntry : eigens)
    {
      for(Entry<Integer, MapVector> inMemoryEntry : inMemoryMatrix)
      {
        if(diskEntry.getKey() - inMemoryEntry.getKey() == 0)
        {
          assertTrue("vector with index : " + diskEntry.getKey() + " is not the same on disk as in memory", 
                     Math.abs(1 - diskEntry.getValue().dot(inMemoryEntry.getValue())) < 1e-6);
        }
        else
        {
          assertTrue("vector with index : " + diskEntry.getKey() 
                     + " is not orthogonal to memory vect with index : " + inMemoryEntry.getKey(),
                     Math.abs(diskEntry.getValue().dot(inMemoryEntry.getValue())) < 1e-6);
        }
      }
    }
    
    eigens.delete();
    
    DiskBufferedDoubleMatrix.delete(new File(TMP_EIGEN_DIR));
  }
  
}
