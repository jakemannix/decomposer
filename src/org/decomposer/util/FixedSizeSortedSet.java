package org.decomposer.util;

import java.util.Comparator;
import java.util.TreeSet;

public class FixedSizeSortedSet<E> extends TreeSet<E>
{
  private static final long serialVersionUID = 1L;
  
  private final Comparator<? super E> _comparator;
  private final int _maxSize;
  
  public FixedSizeSortedSet(int maxSize)
  {
    this(null, maxSize);
  }
  
  public FixedSizeSortedSet(Comparator<? super E> comparator, int maxSize)
  {
    super(comparator);
    _comparator = comparator;
    _maxSize = maxSize;
  }
  
  @Override 
  public boolean add(E e)
  {
    if(size() >= _maxSize)
    {
      E smallest = last();
      int comparison;
      if(_comparator == null) comparison = ((Comparable<E>)e).compareTo(smallest);
      else comparison = _comparator.compare(e, smallest);
      if(comparison > 0)
      {
        remove(smallest);
        return super.add(e);
      }
      return false;
    }
    else
    {
      return super.add(e);
    }
  }
}
