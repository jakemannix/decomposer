package org.decomposer.math.vector.array;

import java.io.Serializable;
import java.util.Iterator;

import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;


import static org.decomposer.math.PrecisionUtils.*;

public class DenseMapVector implements MapVector, Serializable
{
  private static final long serialVersionUID = 1L;
  private double[] _values;
  private double _norm;
  private double _normSquared;
  private boolean _dirtyNorm;
  private boolean _dirtyNormSquared;
  
  DenseMapVector(int initialSize)
  {
    _values = new double[initialSize];
    _norm = 0;
    _normSquared = 0;
    _dirtyNorm = false;
    _dirtyNormSquared = false;
  }
  
  DenseMapVector()
  {
    this(10);
  }

  public final void add(int index, double toBeAdded)
  {
    set(index, get(index) + toBeAdded);
  }

  private void growToAtLeast(int index)
  {
    double[] newValues = new double[2 * index];
    System.arraycopy(_values, 0, newValues, 0, _values.length);
    _values = newValues;
  }

  public double dot(MapVector vector)
  {
    double d = 0;
    if(vector.numNonZeroEntries() < numNonZeroEntries())
    {
      for(IntDoublePair pair : vector)
      {
        d += get(pair.getInt()) * pair.getDouble();
      }
    }
    else
    {
      for(int i=0; i<_values.length; i++)
      {
        d += _values[i] * vector.get(i);
      }
    }
    return d;
  }

  public final double get(int index)
  {
    return index >= _values.length ? 0 : _values[index];
  }

  public final double norm()
  {
    if(_dirtyNorm)
    {
      _normSquared = 0;
      for(double d : _values) _normSquared += d*d;
      _norm = Math.sqrt(_normSquared);
      _dirtyNorm = false;
      _dirtyNormSquared = false;
    }
    return _norm;
  }

  public final double normSquared()
  {
    if(_dirtyNormSquared)
    {
      _normSquared = 0;
      for(double d : _values) _normSquared += d*d;
      _dirtyNormSquared = false;
    }
    return _normSquared;
  }

  public final int numNonZeroEntries()
  {
    return _values.length;
  }
  
  /**
   * Possibly maxDimension is less than this...
   * @return
   */
  public final int maxDimension()
  {
    return numNonZeroEntries();
  }

  public final MapVector plus(MapVector vector)
  {
    for(IntDoublePair pair : vector)
    {
      add(pair.getInt(), pair.getDouble());
    }
    return this;
  }

  public final MapVector plus(MapVector vector, double scale)
  {
    for(IntDoublePair pair : vector)
    {
      add(pair.getInt(), scale * pair.getDouble());
    }
    return this;
  }

  public final MapVector scale(double scale)
  {
    for(int i=0; i<_values.length; i++)
    {
      _values[i] *= scale;
    }
    _norm *= scale;
    _normSquared *= (scale * scale);
    return this;
  }

  public final void set(int index, double value)
  {
    if(index >= _values.length)
      growToAtLeast(index);
    _values[index] = value;
    _dirtyNorm = true;
    _dirtyNormSquared = true;
  }

  public Iterator<IntDoublePair> iterator()
  {
    return new DenseIntDoublePairIterator();
  }
  
  @Override
  public MapVector clone()
  {
    MapVector vector = new DenseMapVector(this.numNonZeroEntries());
    vector.plus(this);
    return vector;
  }
  
  @Override
  public boolean equals(Object other)
  {
    if(!(other instanceof MapVector)) return false;
    MapVector otherVector = (MapVector)other;
    for(IntDoublePair pair : this)
    {
      if(!almostEquals(otherVector.get(pair.getInt()), pair.getDouble())) return false;
    }
    for(IntDoublePair otherPair : otherVector)
    {
      if(!almostEquals(get(otherPair.getInt()), otherPair.getDouble())) return false;
    }
    return true;
  }
  
  private final class DenseIntDoublePairIterator implements Iterator<IntDoublePair>
  {
    DenseIntDoublePair _pair = new DenseIntDoublePair();
    
    public final boolean hasNext()
    {
      return _pair.getInt() < _values.length - 1;
    }

    public final IntDoublePair next()
    {
      _pair._i++;
      return _pair;
    }

    public void remove()
    {
      throw new UnsupportedOperationException();
    }
    
    private final class DenseIntDoublePair implements IntDoublePair
    {
      int _i = -1;
      public final double getDouble() { return _values[_i]; }
      public final int getInt() { return _i; }
      public final void setDouble(double v) { _values[_i] = v; }
    }
    
  }
}
