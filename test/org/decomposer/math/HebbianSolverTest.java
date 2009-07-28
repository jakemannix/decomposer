package org.decomposer.math;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.math.linear.EigenDecomposition;
import org.apache.commons.math.linear.EigenDecompositionImpl;
import org.apache.commons.math.linear.OpenMapRealMatrix;
import org.apache.commons.math.linear.OpenMapRealVector;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.SparseRealMatrix;
import org.apache.commons.math.util.MathUtils;
import org.decomposer.math.LanczosSolver.TimingSection;
import org.decomposer.math.vector.DiskBufferedDoubleMatrix;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.IntDoublePair;
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

  private static DoubleMatrix _previousCorpus = null;
  
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
    return timeSolver(saveDir, 
                      corpusProjectionsVectorFactory, 
                      eigensVectorFactory, 
                      convergence, 
                      maxNumPasses, 
                      10, 
                      state);
  }
  
  public static long timeSolver(String saveDir,
                                VectorFactory corpusProjectionsVectorFactory,
                                VectorFactory eigensVectorFactory,
                                double convergence, 
                                int maxNumPasses,
                                int desiredRank,
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
    _previousCorpus = corpus; 
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
    File corpusDir = new File(TMP_EIGEN_DIR + File.separator + "corpus");
    corpusDir.mkdir();
    DiskBufferedDoubleMatrix.persistChunk(corpusDir, _previousCorpus, true);
   // eigens.delete();
    
   // DiskBufferedDoubleMatrix.delete(new File(TMP_EIGEN_DIR));
  }
  
  public void testHebbianVersusLanczos() throws Exception
  {
    _corpusProjectionsVectorFactory = new DenseMapVectorFactory();
    _eigensVectorFactory = new DenseMapVectorFactory();
    int desiredRank = 200;
    long time = timeSolver(TMP_EIGEN_DIR,
                           _corpusProjectionsVectorFactory, 
                           _eigensVectorFactory,  
                           0.00001, 
                           5, 
                           desiredRank,
                           new TrainingState());

    System.out.println("Hebbian time: " + time + "ms");
    File eigenDir = new File(TMP_EIGEN_DIR + File.separator + HebbianSolver.EIGEN_VECT_DIR);
    DiskBufferedDoubleMatrix eigens = new DiskBufferedDoubleMatrix(eigenDir, 10);
    
    RealMatrix srm = asSparseRealMatrix(_previousCorpus);
    long timeA = System.nanoTime();
    EigenDecomposition asSparseRealDecomp = new EigenDecompositionImpl(srm, 1e-16);
    for(int i=0; i<desiredRank; i++)
      asSparseRealDecomp.getEigenvector(i);
    System.out.println("CommonsMath time: " + (System.nanoTime() - timeA)/TimingConstants.NANOS_IN_MILLI + "ms");
    
   // System.out.println("Hebbian results:");
   // printEigenVerify(eigens, _previousCorpus);
    
    DoubleMatrix lanczosEigenVectors = new HashMapDoubleMatrix(new HashMapVectorFactory());
    List<Double> lanczosEigenValues = new ArrayList<Double>();
 
    LanczosSolver solver = new LanczosSolver();
    solver.solve(_previousCorpus, desiredRank*5, lanczosEigenVectors, lanczosEigenValues);
    
    for(TimingSection section : LanczosSolver.TimingSection.values())
    {
      System.out.println("Lanczos " + section.toString() + " = " + (int)(solver.getTimeMillis(section)/1000) + " seconds");
    }
    
   // System.out.println("\nLanczos results:");
   // printEigenVerify(lanczosEigenVectors, _previousCorpus);
  }
  
  private RealMatrix asSparseRealMatrix(DoubleMatrix corpus)
  {
    SparseRealMatrix srm = new OpenMapRealMatrix((corpus.numRows() * 2), (3 *corpus.numCols())/2 );
    for(Map.Entry<Integer, MapVector> entry : corpus)
    {
      OpenMapRealVector omv = new OpenMapRealVector((3 * corpus.numCols())/2);
      for(IntDoublePair pair : entry.getValue())
      {
        omv.setEntry(pair.getInt(), pair.getDouble());
      }
      srm.setRowVector(entry.getKey(), omv);
    }
    return srm.multiply(srm.transpose());
  }

  public static void printEigenVerify(DoubleMatrix eigens, DoubleMatrix corpus)
  {
    for(Map.Entry<Integer, MapVector> entry : eigens)
    {
      MapVector eigen = entry.getValue();
      MapVector afterMultiply = corpus.timesSquared(eigen);
      double norm = afterMultiply.norm();
      double error = 1 - eigen.dot(afterMultiply) / (eigen.norm() * afterMultiply.norm());
      System.out.println(entry.getKey() + ": error = " + error + ", eVal = " + (norm / eigen.norm()));
    }
  }
}
