package org.decomposer.math.vector.hashmap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;


import static org.decomposer.math.PrecisionUtils.*;


public class HashMapVector implements MapVector, Serializable
{
  private static final long serialVersionUID = 1L;
  protected HashMap<Integer, Double> _map;
  protected int _maxDimension;
  
  HashMapVector()
  {
    this(10);
  }

  HashMapVector(int initialSize) 
  {
    _maxDimension = 0;
    _map = new HashMap<Integer, Double>(initialSize);
  }
  
  HashMapVector(MapVector other)
  {
    this(other.numNonZeroEntries());
    for(IntDoublePair entry : other)
      set(entry.getInt(), entry.getDouble());
  }
  
  public void add(int index, double toBeAdded)
  {
    set(index, get(index) + toBeAdded);
  }

  public double dot(MapVector vector)
  {
    MapVector smaller = vector.numNonZeroEntries() > numNonZeroEntries() ? this : vector;
    MapVector larger = vector.numNonZeroEntries() > numNonZeroEntries() ? vector : this;
    double dot = 0;
    for(IntDoublePair entry : smaller) dot += larger.get(entry.getInt()) * entry.getDouble();
    return dot;
  }

  public double get(int index)
  {
    return _map.containsKey(index) ? _map.get(index) : 0;
  }

  public double norm()
  {
    return Math.sqrt(normSquared());
  }
  
  public double normSquared()
  {
    return this.dot(this);
  }

  public int numNonZeroEntries()
  {
    return _map.size();
  }

  public int maxDimension()
  {
    return _maxDimension;
  }
  
  public MapVector plus(MapVector vector)
  {
    for(IntDoublePair entry : vector)
      add(entry.getInt(), entry.getDouble());
    return this;
  }

  public MapVector scale(double scale)
  {
    for(IntDoublePair entry : this)
    {
      entry.setDouble(entry.getDouble() * scale);
    }
    return this;
  }

  public void set(int index, double value)
  {
    _maxDimension = Math.max(_maxDimension, index); 
    _map.put(index, value);
  }

  public Iterator<IntDoublePair> iterator()
  {
    final Iterator<Map.Entry<Integer, Double>> iter = _map.entrySet().iterator();
    return new Iterator<IntDoublePair>()
    {
      public boolean hasNext() { return iter.hasNext(); }
      public IntDoublePair next()
      {
        final Map.Entry<Integer, Double> entry = iter.next();
        return new IntDoublePair() 
        {
          public double getDouble() { return entry.getValue(); }
          public int getInt() { return entry.getKey(); }
          public void setDouble(double newValue) { entry.setValue(newValue); }          
        };
      }
      public void remove() { iter.remove(); }      
    };
  }

  public MapVector plus(MapVector vector, double scale)
  {
    for(IntDoublePair entry : vector)
      add(entry.getInt(), scale * entry.getDouble());
    return this;
  }
  
  @Override
  public String toString()
  {
    return "{norm:" + norm() + ", nonZeroEntries:" + numNonZeroEntries() + "}";
  }
  
  @Override 
  public boolean equals(Object other)
  {
    if(!(other instanceof MapVector)) return false;
    MapVector otherVector = (MapVector)other;
    for(IntDoublePair pair : this)
    {
      if(!almostEquals(otherVector.get(pair.getInt()), pair.getDouble()))
      {
        return false;
      }
    }
    for(IntDoublePair otherPair : otherVector)
    {
      if(!almostEquals(get(otherPair.getInt()), otherPair.getDouble()))
      {
        return false;
      }
    }
    return true;
  }

  @Override
  public MapVector clone()
  {
    MapVector vector = new HashMapVector();
    vector.plus(this);
    return vector;
  }
}
