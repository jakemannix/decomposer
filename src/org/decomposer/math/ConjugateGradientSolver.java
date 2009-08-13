package org.decomposer.math;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.decomposer.math.vector.DiskBufferedDoubleMatrix;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.array.DenseMapVector;
import org.decomposer.math.vector.array.DenseMapVectorFactory;
import org.decomposer.math.vector.hashmap.HashMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;

public class ConjugateGradientSolver
{
  private final DoubleMatrix corpus;
  private final DoubleMatrix pseudoCorpus;
  public final DoubleMatrix leftVectors;
  public final DoubleMatrix rightVectors;
  private final List<Double> eigenValues;
  private final int rank;
  private final int numRows;
  private final int numCols;
  
  private final double rate;
  private final double smoother;
  private double totalError = 0;
  private int numNonZeroEntries = 0;
  
  public ConjugateGradientSolver(DoubleMatrix inputCorpus, int desiredRank, double rate)
  {
    corpus = inputCorpus;
    pseudoCorpus = new HashMapDoubleMatrix(new HashMapVectorFactory());
    rank = desiredRank;
    numRows = corpus.numRows();
    numCols = corpus.numCols();
    leftVectors = new HashMapDoubleMatrix(new DenseMapVectorFactory());
    rightVectors = new HashMapDoubleMatrix(new DenseMapVectorFactory());
    eigenValues = new ArrayList<Double>();
    this.rate = rate;
    smoother = rate/4;
  }
  
  static int numPasses = 25;
  
  public void solve()
  {
    for(int i=0; i<rank; i++)
    {
      MapVector leftTrainer = initialVector(numRows, 0.1);
      MapVector rightTrainer = initialVector(numCols, 0.1);
      leftVectors.set(i, leftTrainer);
      rightVectors.set(i, rightTrainer);
      orthogonalizeAgainstPrevious(i);
      int max = (int) (numPasses * Math.max(256 * Math.pow(2, -i), 1));
      System.out.println("Going to make " + max + " passes");
      for(int j=0; j<max; j++)
      {
        if(j % 1000 == 0) System.out.println("About to start " + j + "-th pass...");
        for(Entry<Integer, MapVector> row : corpus)
        {
          int rowNum = row.getKey();
          MapVector rowVector = row.getValue();
          if(numNonZeroEntries >= 0)
          {
            numNonZeroEntries += rowVector.numNonZeroEntries();
          }
          for(IntDoublePair pair : rowVector)
          {
            train(rowNum, pair.getInt(), pair.getDouble(), leftTrainer, rightTrainer, j == max - 1);
          }
        }
        if(numNonZeroEntries > 0) numNonZeroEntries *= -1;
        if(j == max - 1)
        {
          System.out.println("RMSE is: " + Math.sqrt(totalError / (-rate * rate * numNonZeroEntries)));
        }
        else
        {
          orthogonalizeAgainstPrevious(i);
        }
        totalError = 0;
      }
      System.out.println("Done training on vectors " + i);
    }
  }

  private void orthogonalizeAgainstPrevious(int i)
  {
    MapVector leftI = leftVectors.get(i);
    MapVector rightI = rightVectors.get(i);
    for(int j=0; j<i; j++)
    {
      MapVector leftJ = leftVectors.get(j);
      MapVector rightJ = rightVectors.get(j);
      double leftIdotJ = leftI.dot(leftJ) / leftJ.normSquared();
      double rightIdotJ = rightI.dot(rightJ) / rightJ.normSquared();
      leftI.plus(leftJ, -leftIdotJ);
      rightI.plus(rightJ, -rightIdotJ);
    }
  }

  private void train(int rowNum, 
                     int colNum, 
                     double actualValue, 
                     MapVector leftTrainer,
                     MapVector rightTrainer,
                     boolean cacheResidual)
  {
    double err = rate * (actualValue - predictValue(rowNum, colNum, leftTrainer, rightTrainer, cacheResidual));
    totalError += (err*err);
    if(cacheResidual) return;
    double currL = leftTrainer.get(rowNum);
    double currR = rightTrainer.get(colNum);
    leftTrainer.set(rowNum, currL + (err * currR - smoother * currL));
    rightTrainer.set(colNum, currR + (err * currL - smoother * currR));
  }

  private double predictValue(int rowNum, int colNum, MapVector leftTrainer, MapVector rightTrainer, boolean cacheResidual)
  {
    MapVector row = pseudoCorpus.get(rowNum);
    if(row == null)
    {
      row = new HashMapVector();
      pseudoCorpus.set(rowNum, row);
    }
    double predicted = row.get(colNum);
    predicted += (leftTrainer.get(rowNum) * rightTrainer.get(colNum));
    if(cacheResidual)
    {
      row.set(colNum, predicted);
    }
    return predicted;
  }
  
  private MapVector initialVector(int dimension, double value)
  {
    double[] vals = new double[dimension];
    Arrays.fill(vals, value);
    return new DenseMapVector(vals);
  }
  
  public static void main(String[] args) throws Exception
  {
    DoubleMatrix corpus = DiskBufferedDoubleMatrix.loadFullMatrix(new File("contrib/hadoop/test/data/random_matrix/serialized_format/corpus"), 
                                                                  new HashMapVectorFactory());
    
//    corpus = TestDoubleMatrix.randomImmutableSparseDoubleMatrix(5000, 5000, 600, 30, 5.0);
    ConjugateGradientSolver solver = new ConjugateGradientSolver(corpus, 25, 0.0004);
    solver.solve();
    DoubleMatrix rightVectors = solver.rightVectors;
//    for(int i=0; i<rightVectors.numRows(); i++)
//    {
//      MapVector ei = rightVectors.get(i);
//      for(int j=0; j<i; j++)
//      {
//        MapVector ej = rightVectors.get(j);
//        double dot = ei.dot(ej) / (ei.norm() * ej.norm());
//        System.out.println("conjugateGradient: e["+i+"] * e["+j+"] = " + dot);
//      }
//    }
    for(Entry<Integer, MapVector> right : rightVectors)
    {
      MapVector after = corpus.timesSquared(right.getValue());
      double dot = 1 - after.dot(right.getValue()) / (after.norm() * right.getValue().norm());
      System.out.println("1-cos(theta_CG) [" + right.getKey() + "] = " + dot + ", singularValue = " + (after.norm()/right.getValue().norm()));
    }
    
    rightVectors = DiskBufferedDoubleMatrix.loadFullMatrix(new File("contrib/hadoop/test/data/random_matrix/serialized_format/eigenVectors"),
                                                           new DenseMapVectorFactory());

    for(Entry<Integer, MapVector> right : rightVectors)
    {
      MapVector after = corpus.timesSquared(right.getValue());
      double dot = 1 - after.dot(right.getValue()) / (after.norm() * right.getValue().norm());
      System.out.println("1-cos(theta_Hebbian) [" + right.getKey() + "] = " + dot + ", singularValue = " + (after.norm()/right.getValue().norm()));
    }
    System.exit(0);
  }
}
