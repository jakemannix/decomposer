package org.decomposer.math.vector;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.decomposer.math.vector.array.DenseMapVectorFactory;

public class ParallelMultiplyingDiskBufferedDoubleMatrix extends DiskBufferedDoubleMatrix
{
  private static final long serialVersionUID = 1L;
  
  protected final ExecutorService _threadPool;
  protected final int _numThreads;

  public ParallelMultiplyingDiskBufferedDoubleMatrix(File diskDir,
                                                     int pageSize,
                                                     boolean isRowMatrix,
                                                     int numThreads)
  {
    super(diskDir, pageSize, isRowMatrix);
    _numThreads = numThreads;
    _threadPool = Executors.newFixedThreadPool(numThreads);
  }

  public ParallelMultiplyingDiskBufferedDoubleMatrix(File diskDir, int pageSize, int numThreads)
  {
    super(diskDir, pageSize);
    _numThreads = numThreads;
    _threadPool = Executors.newFixedThreadPool(numThreads);
  }
  
  @Override
  public MapVector timesSquared(MapVector input)
  {
    if(!_isNumColsCalculated) for(Entry<Integer, MapVector> e : this); // just iterate, to calculate numCols()
    
    MapVector output = new DenseMapVectorFactory().zeroVector(numCols());
    List<Future<MapVector>> subVectorFutures = new ArrayList<Future<MapVector>>(_numThreads);
    for(int i=0; i<_numThreads; i++)
    {
      subVectorFutures.add(_threadPool.submit(new MultiplierCallable(input, this, _numThreads, i)));      
    }
    List<MapVector> subVectors = new ArrayList<MapVector>(_numThreads);
    for(Future<MapVector> future : subVectorFutures)
    {
      try
      {
        subVectors.add(future.get());
      }
      catch (Exception e)
      {
        throw new RuntimeException(e);
      }
    }
    for(MapVector subVector : subVectors) output.plus(subVector);
    return output;
  }

  protected static final class MultiplierCallable implements Callable<MapVector>
  {
    protected final MapVector _output;
    protected final MapVector _input;
    protected final ParallelMultiplyingDiskBufferedDoubleMatrix _matrix;
    protected final int _threadNum;
    protected final int _numThreads;
    
    public MultiplierCallable(MapVector input, ParallelMultiplyingDiskBufferedDoubleMatrix m, int numThreads, int i)
    {
      _matrix = m;
      _input = input;
      _output = new DenseMapVectorFactory().zeroVector(m.numCols());
      _threadNum = i;
      _numThreads = numThreads;
    }

    public MapVector call() throws Exception
    {
      for(Entry<Integer, MapVector> entry : _matrix)
      {
        if(entry.getKey() % _numThreads == _threadNum) _output.plus(entry.getValue(), entry.getValue().dot(_input));
      }
      return _output;
    }
    
  }
  
}
