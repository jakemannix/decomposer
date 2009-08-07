package org.decomposer.math.vector.array;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;


import static org.decomposer.math.PrecisionUtils.*;

public class ImmutableSparseMapVector implements MapVector, Serializable
{
  private static final long serialVersionUID = 1L;
  protected final int[] _indices;
  protected final double[] _values;
  protected double _norm;
  protected double _normSquared;
  
  public ImmutableSparseMapVector(int[] indices, double[] values)
  {
    _indices = indices;
    _values = values;
    double n = 0;
    for(double d : _values)
    {
      n += d*d;
    }
    _normSquared = n;
    _norm = Math.sqrt(_normSquared);
  }
  
  public ImmutableSparseMapVector(MapVector other)
  {
    if(other == null)
    {
      _indices = new int[0];
      _values = new double[0];
      _norm = 0;
      _normSquared = 0;
      return;
    }
    if(other instanceof ImmutableSparseMapVector)
    {
      ImmutableSparseMapVector immutableOther = (ImmutableSparseMapVector)other;
      _indices = immutableOther._indices;
      _values = immutableOther._values;
      _norm = immutableOther._norm;
      _normSquared = immutableOther._normSquared;
      return;
    }
    SortedSet<IntDoublePair> sorted = new TreeSet<IntDoublePair>(new Comparator<IntDoublePair>()
        {
          public int compare(IntDoublePair o1, IntDoublePair o2)
          {
            return o1.getInt() - o2.getInt();
          }
        });
    for(IntDoublePair pair : other)
    {
      final int i = pair.getInt();
      final double d = pair.getDouble();
      sorted.add(new IntDoublePair()
      {
        public double getDouble() { return d; }
        public int getInt() { return i; }
        public void setDouble(double d) { throw new UnsupportedOperationException(getClass().getName() + " is immutable"); }
      });
    }
    _indices = new int[sorted.size()];
    _values = new double[sorted.size()];
    int index = 0;
    for(IntDoublePair pair : sorted)
    {
      _indices[index] = pair.getInt();
      _values[index] = pair.getDouble();
      index++;
    }
    double n = 0;
    for(double d : _values)
    {
      n += d*d;
    }
    _normSquared = n;
    _norm = Math.sqrt(_normSquared);
  }
  
  public void add(int index, double toBeAdded)
  {
    throw new UnsupportedOperationException(getClass().getName() + " is immutable");
  }

  public double dot(MapVector vector)
  {
    double dot = 0;
    if(vector instanceof ImmutableSparseMapVector)
    {
      ImmutableSparseMapVector other = (ImmutableSparseMapVector)vector;
      int i = 0;
      int j = 0;
      while(i<_indices.length && j<other._indices.length)
      {
        while(i<_indices.length && _indices[i] < other._indices[j] ) i++;
        if(i<_indices.length)
          while(j<other._indices.length && _indices[i] > other._indices[j] ) j++;
        else
          break;
        if(j>= other._indices.length)
          break;
        if(_indices[i] == other._indices[j])
        {
          dot += _values[i] * other._values[j];
          i++;
          j++;
        }
      }
    }
    else
    {
      for(IntDoublePair pair : this)
      {
        dot += vector.get(pair.getInt()) * pair.getDouble();
      }
    }
    return dot;
  }

  public double get(int index)
  {
    int found = Arrays.binarySearch(_indices, index);
    return (found >= 0) ? _values[found] : 0;
  }

  public double norm()
  {
    return _norm;
  }

  public double normSquared()
  {
    return _normSquared;
  }

  public int numNonZeroEntries()
  {
    return _indices.length;
  }
  
  public int maxDimension()
  {
    return _indices.length > 0 ? _indices[_indices.length - 1] : 0;
  }

  public MapVector plus(MapVector vector)
  {
    throw new UnsupportedOperationException(getClass().getName() + " is immutable");
  }

  public MapVector plus(MapVector vector, double scale)
  {
    throw new UnsupportedOperationException(getClass().getName() + " is immutable");
  }

  public MapVector scale(double scale)
  {
    throw new UnsupportedOperationException(getClass().getName() + " is immutable");
  }
  
  public MapVector scaleOverride(double scale)
  {
    for(int i=0; i<_values.length; i++)
      _values[i] = _values[i] * scale;
    _norm *= scale;
    _normSquared *= (scale * scale);
    return this;
  }

  public void set(int index, double value)
  {
    throw new UnsupportedOperationException(getClass().getName() + " is immutable");
  }

  public Iterator<IntDoublePair> iterator()
  {
    return new ImmutableIntDoublePairIterator();
  }
  
  @Override
  public MapVector clone()
  {
    // since this class is supposed to be immutable, returning itself as a clone should be ok
    return this;
  }
  
  public String toString()
  {
    return MapVector.F.toString(this);
  }
  
  
  @Override
  public boolean equals(Object other)
  {
    if(other == null)
    {
      return norm() == 0;
    }
    else
    {
      if(!(other instanceof MapVector)) return false;
      MapVector otherVector = (MapVector)other;
      for(IntDoublePair pair : this)
      {
        if(!almostEquals(pair.getDouble(), otherVector.get(pair.getInt()))) return false;
      }
      for(IntDoublePair otherPair : otherVector)
      {
        if(!almostEquals(otherPair.getDouble(), get(otherPair.getInt()))) return false;
      }
      return true;
    }
  }
  
  protected final class ImmutableIntDoublePairIterator implements Iterator<IntDoublePair>
  {
    final ImmutableIntDoublePair _pair = new ImmutableIntDoublePair();
    
    public boolean hasNext()
    {
      return _pair._i < _indices.length - 1;
    }

    public IntDoublePair next()
    {
      _pair._i++;
      return _pair;
    }

    public void remove()
    {
      throw new UnsupportedOperationException(getClass().getName() + " is immutable");
    }
    
    protected final class ImmutableIntDoublePair implements IntDoublePair
    {
      int _i = -1;
      public final double getDouble() { return _values[_i]; }
      public final int getInt() { return _indices[_i]; }
      public final void setDouble(double v) 
      {
        throw new UnsupportedOperationException(getClass().getName() + " is immutable");
      }
    }
    
  }
}
