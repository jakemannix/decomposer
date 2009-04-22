package org.decomposer.math;

import java.io.File;
import java.util.Map.Entry;

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

  private VectorFactory _corpusProjectionsVectorFactory;
  private VectorFactory _eigensVectorFactory;
  
  public static final String TMP_EIGEN_DIR = "tmp.HebbianSolverTest.eigenDir";
  
  public HebbianSolverTest(String name)
  {
    super(name);
  }

  public static long timeSolver(String saveDir,
                                VectorFactory corpusProjectionsVectorFactory,
                                VectorFactory eigensVectorFactory,
                                double convergence, 
                                int maxNumPasses,
                                TrainingState state) throws Exception
  {
    long time = 0;
    HebbianUpdater updater = new HebbianUpdater();
    SingularVectorVerifier verifier = new MultiThreadedEigenVerifier();
    HebbianSolver solver;
    solver = new HebbianSolver(updater, 
                               corpusProjectionsVectorFactory,
                               eigensVectorFactory, 
                               verifier, 
                               saveDir, 
                               convergence, 
                               maxNumPasses);
    DoubleMatrix corpus = TestDoubleMatrix.randomImmutableSparseDoubleMatrix(2000, 1900, 1600, 100, 1.0);
    int desiredRank = 10;
    long start = System.nanoTime();
    TrainingState finalState = solver.solve(corpus, desiredRank);
    //Thread.sleep(5000);
    assertNotNull(finalState);
    state.currentEigens = finalState.currentEigens;
    state.currentEigenValues = finalState.currentEigenValues;
    time += System.nanoTime() - start;
    assertEquals(state.currentEigens.numRows(), desiredRank);
    assertOrthonormal(state.currentEigens);
    return (long)(time / 1e6);
  }
  
  private static void assertOrthonormal(DoubleMatrix currentEigens)
  {
    for(int i=0; i<currentEigens.numRows(); i++)
    {
      MapVector ei = currentEigens.get(i);
      for(int j=0; j<currentEigens.numRows(); j++)
      {
        MapVector ej = currentEigens.get(j);
        double dot = ei.dot(ej);
        if(i == j)
        {
          assertTrue("not norm 1 : " + dot, Math.abs(1-dot) < 1e-6);
        }
        else
        {
          assertTrue("not orthogonal : " + dot, Math.abs(dot) < 1e-6);
        }
      }
    }
  }
  
  public static long timeSolver(TrainingState state) throws Exception
  {
    return timeSolver(null, new DenseMapVectorFactory(), new DenseMapVectorFactory(), 0.01, 5, state);
  }
  
  public void testHebbianSolver() throws Exception
  {
    _corpusProjectionsVectorFactory = new DenseMapVectorFactory();
    _eigensVectorFactory = new DenseMapVectorFactory();
    
    long optimizedTime = timeSolver(null, 
                                    _corpusProjectionsVectorFactory, 
                                    _eigensVectorFactory, 
                                    0.00001, 
                                    5, 
                                    new TrainingState());

    _corpusProjectionsVectorFactory = new HashMapVectorFactory();
    _eigensVectorFactory = new HashMapVectorFactory();
    
    long unoptimizedTime = timeSolver(null,
                                      _corpusProjectionsVectorFactory, 
                                      _eigensVectorFactory,  
                                      0.00001, 
                                      5, 
                                      new TrainingState());
    
    System.out.println("Avg solving (unoptimized) time in ms:" + unoptimizedTime);
    System.out.println("Avg solving   (optimized) time in ms:" + optimizedTime);    
  }
  
  public void testSolverWithSerialization() throws Exception
  {
    _corpusProjectionsVectorFactory = new DenseMapVectorFactory();
    _eigensVectorFactory = new DenseMapVectorFactory();
    
    timeSolver(TMP_EIGEN_DIR,
               _corpusProjectionsVectorFactory, 
               _eigensVectorFactory,  
               0.001, 
               5, 
               new TrainingState());
    
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
