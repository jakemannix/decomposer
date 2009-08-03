package org.decomposer.math.vector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.decomposer.math.vector.array.DenseMapVectorFactory;

public class HashMapDoubleMatrix implements DoubleMatrix, Serializable
{
  private static final long serialVersionUID = 1L;
  protected Map<Integer, MapVector> _map;
  protected transient VectorFactory _vectorFactory;
  protected int _numCols = 0;
  /**
   * 
   * @param vectorFactory is used for creating new vectors when calling <code>times(MapVector vector)</code>, and <code>transpose()</code>
   */
  public HashMapDoubleMatrix(VectorFactory vectorFactory)
  {
    _map = new HashMap<Integer, MapVector>();
    _vectorFactory = vectorFactory;
  }
  
  public HashMapDoubleMatrix()
  {
    this(new DenseMapVectorFactory());
  }
  
  public HashMapDoubleMatrix(DoubleMatrix other)
  {
    this(other instanceof HashMapDoubleMatrix ? ((HashMapDoubleMatrix)other)._vectorFactory : new DenseMapVectorFactory(), other);
  }
  
  public HashMapDoubleMatrix(VectorFactory vectorFactory, DoubleMatrix other)
  {
    this(vectorFactory);
    for(Entry<Integer, MapVector> entry : other)
      set(entry.getKey(), entry.getValue());
  }
  
  public MapVector get(int rowNumber)
  {
    return _map.get(rowNumber);
  }

  public DoubleMatrix scale(double scale)
  {
    for(Map.Entry<Integer, MapVector> vectorEntry : this)
    {
      vectorEntry.getValue().scale(scale);
    }
    return this;
  }

  public void set(int rowNumber, MapVector row)
  {
    _map.put(rowNumber, row);
  }

  public MapVector times(MapVector vector)
  {
    MapVector output = _vectorFactory.zeroVector();
    if(vector != null)
    {
      for(Map.Entry<Integer, MapVector> entry : this)
      {
        output.set(entry.getKey(), vector.dot(entry.getValue()));
      }
    }
    return output;
  }

  public Iterator<Entry<Integer, MapVector>> iterator()
  {
    return _map.entrySet().iterator();
  }
  
  @Override
  public boolean equals(Object object)
  {
    if(!(object instanceof DoubleMatrix))
      return false;
    DoubleMatrix other = (DoubleMatrix)object;
    for(Map.Entry<Integer, MapVector> entry : this)
    {
      MapVector otherVector = other.get(entry.getKey());
      if(!entry.getValue().equals(otherVector)) return false;
    }
    return true;
  }

  public int numRows()
  {
    return _map.size();
  }
  
  public int numCols()
  {
    if(_numCols <= 0)
      for(Map.Entry<Integer, MapVector> entry : this)
        _numCols = Math.max(_numCols, entry.getValue().maxDimension());
    return _numCols;
  }
  
  public String toString()
  {
    StringBuffer buf = new StringBuffer();
    buf.append("{ ");
    Iterator<Entry<Integer, MapVector>> it = iterator();
    while(it.hasNext())
    {
      Entry<Integer, MapVector> entry = it.next();
      buf.append(entry.getKey() + ":" + entry.getValue());
      if(it.hasNext()) buf.append(", ");
    }
    buf.append("}");
    return buf.toString();
  }

  public MapVector timesSquared(MapVector vector)
  {
    MapVector w = times(vector);
    MapVector w2 = _vectorFactory.zeroVector(numCols());
    for(Entry<Integer, MapVector> entry : this)
    {
      w2.plus(entry.getValue(), w.get(entry.getKey()));
    }
    return w2;
  }

}
