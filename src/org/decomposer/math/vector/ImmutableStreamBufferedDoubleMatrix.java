package org.decomposer.math.vector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.decomposer.math.vector.array.DenseMapVectorFactory;


public abstract class ImmutableStreamBufferedDoubleMatrix implements RowSortedDoubleMatrix
{
  private static final long serialVersionUID = 1L;
  private static final Logger log = Logger.getLogger(ImmutableStreamBufferedDoubleMatrix.class.getName());
  protected VectorFactory _vectorFactory = new DenseMapVectorFactory();
  protected ExecutorService _threadPool = Executors.newCachedThreadPool();
  protected boolean _isNumColsCalculated = false;
  protected int _numCols = 0;
  
  public MapVector get(int rowNumber)
  {
    throw new UnsupportedOperationException(getClass().getName() + " is only accessed via an iterator");
  }

  public abstract int getPageSize();
  
  public abstract int numRows();
  
  public int numCols()
  {
    if(_isNumColsCalculated) 
    {
      return _numCols;
    }
    else
    {
      throw new IllegalStateException("Must first make one pass through to calculate numCols");
    }
  }

  public DoubleMatrix scale(double scale)
  {
    throw new UnsupportedOperationException(getClass() + " is immutable");
  }

  public void set(int rowNumber, MapVector row)
  {
    throw new UnsupportedOperationException(getClass() + " is immutable");
  }

  public MapVector times(MapVector vector)
  {
    MapVector output = _vectorFactory.zeroVector();
    for(Entry<Integer, MapVector> vectorEntry : this)
    {
      output.set(vectorEntry.getKey(), vectorEntry.getValue().dot(vector));
    }
    return output;
  }

  public Iterator<Entry<Integer, MapVector>> iterator()
  {
    return new BufferingIterator(0, getPageSize());
  }
  
  /**
   * 
   * @param offset
   * @param count
   * @return can be null if there are no more after <code>offset</code>
   */
  protected abstract Iterator<Entry<Integer, MapVector>> newBuffer(int offset, int count);

  protected class BufferingIterator implements Iterator<Entry<Integer, MapVector>>
  {
    protected Iterator<Entry<Integer, MapVector>> _currentIterator;
    protected Iterator<Entry<Integer, MapVector>> _nextIterator;
    protected Future<Iterator<Entry<Integer, MapVector>>> _futureIterator;
    protected int _offset;
    protected int _pageSize;
    
    BufferingIterator(int offset, int pageSize)
    {
      _offset = offset;
      _pageSize = pageSize;
      _currentIterator = newBuffer(offset, pageSize);
      _nextIterator = newBuffer(offset + pageSize, pageSize);
      _offset += 2 * pageSize;
    }
    
    public boolean hasNext()
    {
      // Most common case: current buffer hasNext(), return true
      if(_currentIterator != null && _currentIterator.hasNext()) 
      {
        return true;
      }
      // when the currentIterator runs out, try the next one, if it hasNext(), swap it into current, and asyncRefill() the next
      else if(_nextIterator != null && _nextIterator.hasNext())
      {
        _currentIterator = _nextIterator;
        _nextIterator = null;
        List<Entry<Integer, MapVector>> trimmed = new ArrayList<Entry<Integer, MapVector>>();
        while(_currentIterator.hasNext())
        {
          Entry<Integer, MapVector> entry = _currentIterator.next();
          if(entry.getKey() >= _offset) trimmed.add(entry);
        }
        _currentIterator = trimmed.size() > 0 ? trimmed.iterator() : null;
        asyncRefill();
        return hasNext();
      }
      // asyncRefill hasn't yet completed
      else
      {
        try
        {
          if(_futureIterator != null)
          {
            _nextIterator = _futureIterator.get();
            _futureIterator = null;
            return hasNext();
          }
          // newBuffer returned null, there's no more buffers to refill, matrix is finished iterating.
          else
          {
            return false;
          }
        }
        catch (InterruptedException e)
        {
          log.warning(e.getMessage());
          return false;
        }
        catch (ExecutionException e)
        {
          log.severe(e.getMessage());
          return false;
        }
      }
    }

    public Entry<Integer, MapVector> next()
    {
      Entry<Integer, MapVector> next = (_currentIterator != null) ? _currentIterator.next() : null;
      _offset = next != null ? (next.getKey()+1) : Integer.MAX_VALUE;
      return next;
    }
    
    protected void asyncRefill()
    {
      _futureIterator = _threadPool.submit(iteratorCallable(_offset, _pageSize));
    }
    
    public Callable<Iterator<Entry<Integer, MapVector>>> iteratorCallable(final int offset, final int pageSize)
    {
      return new Callable<Iterator<Entry<Integer, MapVector>>>()
      {
        public Iterator<Entry<Integer, MapVector>> call() throws Exception
        {
          return newBuffer(offset, pageSize);
        }   
      };
    }

    public void remove()
    {
      throw new UnsupportedOperationException(getClass() + " is immutable");
    }
  }

  @Override
  public boolean equals(Object other)
  {
    if(!(other instanceof DoubleMatrix)) return false;
    if(other instanceof RowSortedDoubleMatrix)
    {
      RowSortedDoubleMatrix otherMatrix = (RowSortedDoubleMatrix)other;
      Iterator<Entry<Integer, MapVector>> otherIt = otherMatrix.iterator();
      Iterator<Entry<Integer, MapVector>> it = iterator();
      while(it.hasNext() && otherIt.hasNext())
      {
        Entry<Integer, MapVector> next = it.next();
        Entry<Integer, MapVector> otherNext = otherIt.next();
        if(next.getKey() != otherNext.getKey() || !next.getValue().equals(otherNext.getValue())) return false;
      }
      return !it.hasNext() && !otherIt.hasNext();
    }
    else
    {
      DoubleMatrix otherMatrix = (DoubleMatrix)other;
      Set<Integer> checkedRows = new HashSet<Integer>();
      Iterator<Entry<Integer, MapVector>> it = iterator();
      Entry<Integer, MapVector> itEntry;
      while(it.hasNext() && (itEntry = it.next()) != null)
      {
        checkedRows.add(itEntry.getKey());
        MapVector otherVector = otherMatrix.get(itEntry.getKey());
        if(!itEntry.getValue().equals(otherVector)) 
        {
          return false;
        }
      }
      for(Entry<Integer, MapVector> entry : otherMatrix)
      {
        if(!checkedRows.contains(entry.getKey()) && entry.getValue().norm() > 0) 
        {
          return false;
        }
      }
      return true;
    }
  }
}
