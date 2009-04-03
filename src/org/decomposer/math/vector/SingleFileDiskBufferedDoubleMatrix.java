package org.decomposer.math.vector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SingleFileDiskBufferedDoubleMatrix extends ImmutableStreamBufferedDoubleMatrix
{

  private static final long serialVersionUID = 1L;
  public static final String MATRIX_FILE = "matrix.ser";
  public static final String INDEX_FILE = "matrix.idx";
  protected final SortedMap<Integer, Integer> docIndexOffsets = new TreeMap<Integer, Integer>();
  protected ExecutorService _threadPool = Executors.newCachedThreadPool();
  protected final File _dir;
  protected File _matrixFile;
  protected int _pageSize;

  public SingleFileDiskBufferedDoubleMatrix(File dir, int pageSize)
  {
    _dir = dir;
    _pageSize = pageSize;
    if(!_dir.isDirectory()) throw new IllegalArgumentException(_dir + " is not a filesystem directory");
    for(File file : _dir.listFiles())
    {
      if(file.getName().equals(MATRIX_FILE))
      {
        _matrixFile = file;
      }
      if(file.getName().equals(INDEX_FILE))
      {
        buildIndex(file);
      }
    }
  }

  /**
   * fills in the <code>docIndexOffsets</code> map
   * @param file
   */
  private void buildIndex(File file)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int getPageSize()
  {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  protected Iterator<Entry<Integer, MapVector>> newBuffer(int offset, int count)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int numRows()
  {
    // TODO Auto-generated method stub
    return 0;
  }

  public MapVector timesSquared(MapVector vector)
  {
    // TODO Auto-generated method stub
    return null;
  }

  protected class SingleFileBufferingIterator implements Iterator<Entry<Integer, MapVector>>
  {
    int _pageSize;
    ObjectInputStream _inputStream;
    
    int _byteOffset;
    
    List<Entry<Integer, MapVector>> _currentBuffer;
    int _bufferOffset;
    
    List<Entry<Integer, MapVector>> _nextBuffer;
    Future<List<Entry<Integer, MapVector>>> _futureBuffer;
    
    SingleFileBufferingIterator(int offset, int pageSize) throws FileNotFoundException, IOException, ClassNotFoundException
    {
      _pageSize = pageSize;
      _inputStream = new ObjectInputStream(new FileInputStream(_matrixFile));
      _byteOffset = 0;
      _bufferOffset = 0;
      _currentBuffer = refillBuffer(offset + pageSize, pageSize);
      _nextBuffer = refillBuffer(offset + 2 * pageSize, pageSize);
    }

    private List<Entry<Integer, MapVector>> refillBuffer(int minIndexToRead, int pageSize) throws IOException, ClassNotFoundException
    {
      List<Entry<Integer, MapVector>> list = new ArrayList<Entry<Integer, MapVector>>();
      Entry<Integer, MapVector> entry = null;
      while((entry = (Entry<Integer, MapVector>) _inputStream.readObject()) != null)
      {
        list.add(entry);
        if(entry.getKey() > minIndexToRead && list.size() > pageSize) break;
      }
      return list;
    }
    
    protected void asyncRefill()
    {
      _futureBuffer = _threadPool.submit(iteratorCallable(highestIndexRead() + _pageSize));
    }
    
    public Callable<List<Entry<Integer, MapVector>>> iteratorCallable(final int minIndexToRead)
    {
      return new Callable<List<Entry<Integer, MapVector>>>()
      {
        public List<Entry<Integer, MapVector>> call() throws Exception
        {
          return refillBuffer(minIndexToRead, _pageSize);
        }   
      };
    }
    
    private int highestIndexRead()
    {
      if(_currentBuffer == null) return 0;
      if(!_currentBuffer.isEmpty())
      {
        return _currentBuffer.get(_currentBuffer.size() - 1).getKey();
      }
      else
      {
        return Integer.MAX_VALUE;
      }
    }

    public boolean hasNext()
    {
      if(_currentBuffer != null && _bufferOffset < _currentBuffer.size())
      {
        return true;
      }
      else if(_nextBuffer != null && !_nextBuffer.isEmpty())
      {
        _currentBuffer = _nextBuffer;
        _nextBuffer = null;
        _bufferOffset = 0;
        asyncRefill();
        return hasNext();
      }
      else
      {
        try
        {
          if(_futureBuffer != null)
          {
            _nextBuffer = _futureBuffer.get();
            _futureBuffer = null;
            return hasNext();
          }
          else
          {
            return false;
          }
        }
        catch(InterruptedException ie)
        {
          
          return false;
        }
        catch(ExecutionException ee)
        {
          
          return false;
        }
      }
    }

    public Entry<Integer, MapVector> next()
    {
      Entry<Integer, MapVector> next = _currentBuffer != null && _currentBuffer.size() > _bufferOffset ? _currentBuffer.get(_bufferOffset) : null;
      _bufferOffset++;
      return next;
    }

    public void remove()
    {
      throw new UnsupportedOperationException(getClass() + " is immutable, does not support Iterator#remove()");
    }
    
  }
}
