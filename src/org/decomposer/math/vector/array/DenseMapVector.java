package org.decomposer.math.vector.array;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;

import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;


import static org.decomposer.math.PrecisionUtils.*;

public class DenseMapVector implements MapVector, Serializable
{
  private static final long serialVersionUID = 1L;
  private double[] _values;
  private int _maxDimension;
  private double _norm;
  private double _normSquared;
  private boolean _dirtyNorm;
  private boolean _dirtyNormSquared;
  
  public DenseMapVector(double[] values)
  {
    _values = values;
    _maxDimension = values.length;
    _norm = 0;
    _normSquared = 0;
    _dirtyNorm = true;
    _dirtyNormSquared = true;
  }
  
  public DenseMapVector(int initialSize)
  {
    _values = new double[initialSize];
    _maxDimension = -1;
    _norm = 0;
    _normSquared = 0;
    _dirtyNorm = false;
    _dirtyNormSquared = false;
  }
  
  public DenseMapVector()
  {
    this(10);
  }

  public DenseMapVector(DenseMapVector vector)
  {
    this(vector._values);
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
      for(int i=0; i<_values.length; i++)
      {
        double d = _values[i];
        if(d != 0) _maxDimension = i;
        _normSquared += d*d;
      }
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
      for(int i=0; i<_values.length; i++)
      {
        double d = _values[i];
        if(d != 0) _maxDimension = i;
        _normSquared += d*d;
      }
      _dirtyNormSquared = false;
    }
    return _normSquared;
  }

  /**
   * Possibly maxDimension is less than this...
   * @return the length of the underlying value array
   */
  public final int numNonZeroEntries()
  {
    return _values.length;
  }
  
  /**
   * @return the highest dimension index which is nonzero
   */
  public final int maxDimension()
  {
    if(_dirtyNormSquared) normSquared();
    return _maxDimension;
  }

  public final MapVector plus(MapVector vector)
  {
    for(IntDoublePair pair : vector)
    {
      if(pair.getDouble() != 0) add(pair.getInt(), pair.getDouble());
    }
    return this;
  }

  public final MapVector plus(MapVector vector, double scale)
  {
    if(scale == 0) return this;
    for(IntDoublePair pair : vector)
    {
      if(pair.getDouble() != 0) add(pair.getInt(), scale * pair.getDouble());
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
  
  private void writeObject(ObjectOutputStream out) throws IOException
  {
    normSquared();
    out.writeInt(_maxDimension);
    out.writeDouble(_norm);
    for(int i=0; i<=_maxDimension; i++) out.writeDouble(_values[i]);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    _maxDimension = in.readInt();
    _norm = in.readDouble();
    _normSquared = _norm * _norm;
    _dirtyNorm = false;
    _dirtyNormSquared = false;
    _values = new double[_maxDimension+1];
    for(int i=0; i<=_maxDimension; i++) _values[i] = in.readDouble();
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
  
  public String toString()
  {
    return MapVector.F.toString(this);
  }
  
  private final class DenseIntDoublePairIterator implements Iterator<IntDoublePair>
  {
    DenseIntDoublePair _pair = new DenseIntDoublePair();
    final int max = Integer.MAX_VALUE;
    public DenseIntDoublePairIterator()
    {
      //max = maxDimension();
    }
    
    public final boolean hasNext()
    {
      return _pair._i < max - 1 && _pair._i < _values.length - 1;
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
