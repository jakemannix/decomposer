package org.decomposer.util;

import java.util.Comparator;
import java.util.TreeSet;

public class FixedSizeSortedSet<E> extends TreeSet<E>
{
  private static final long serialVersionUID = 1L;
  
  private final Comparator<? super E> _comparator;
  private final int _maxSize;
  
  public FixedSizeSortedSet(Comparator<? super E> comparator, int maxSize)
  {
    _comparator = comparator;
    _maxSize = maxSize;
  }
  
  @Override 
  public boolean add(E e)
  {
    if(size() >= _maxSize)
    {
      E smallest = last();
      if(_comparator.compare(e, smallest) > 0)
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
