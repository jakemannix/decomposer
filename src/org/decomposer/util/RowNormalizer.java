package org.decomposer.util;

import java.io.File;
import java.util.Map.Entry;

import org.decomposer.math.vector.DiskBufferedDoubleMatrix;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;

/**
 * Horrible hack for re-normalizing the rows in a matrix to unit length.
 * @author jmannix
 *
 */
public class RowNormalizer
{
  
  public static void main(String[] args)
  {
    try
    {
    int inputBufferSize = Integer.parseInt(args[2]);
    int outputBufferSize = Integer.parseInt(args[3]);
    DiskBufferedDoubleMatrix inputMatrix = new DiskBufferedDoubleMatrix(new File(args[0]), inputBufferSize);
    VectorFactory hashMapVectorFactory = new HashMapVectorFactory();
    DoubleMatrix tmpMatrix = new HashMapDoubleMatrix(hashMapVectorFactory);
    for(Entry<Integer, MapVector> row : inputMatrix)
    {
      int rowNum = row.getKey();
      MapVector v = row.getValue();
      double rtNorm = Math.sqrt(v.norm());
      ImmutableSparseMapVector scaledV = new ImmutableSparseMapVector(v);
      if(rtNorm > 0) scaledV.scaleOverride(1/rtNorm);
      tmpMatrix.set(rowNum, scaledV);
      if(tmpMatrix.numRows() > outputBufferSize)
      {
        DiskBufferedDoubleMatrix.persistChunk(new File(args[1]), tmpMatrix, true);
        tmpMatrix = new HashMapDoubleMatrix(hashMapVectorFactory);
      }
    }
    if(tmpMatrix.numRows() > 0) DiskBufferedDoubleMatrix.persistChunk(new File(args[1]), tmpMatrix, true);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.out.println("Usage: java -cp jarpath org.decomposer.util.RowNormalizer inDir outDir inBufSize outBufSize");
    }
  }
}
