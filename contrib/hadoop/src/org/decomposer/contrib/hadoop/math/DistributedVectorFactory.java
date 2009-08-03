package org.decomposer.contrib.hadoop.math;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.DenseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVector;

public class DistributedVectorFactory implements VectorFactory, Configurable
{
  protected Configuration conf;
  protected boolean sparse;
  
  public DistributedVectorFactory()
  {
    this(false);
  }
  
  public DistributedVectorFactory(boolean sparse)
  {
    this.sparse = sparse;
  }
  
  @Override
  public MapVector zeroVector()
  {
    return zeroVector(getConf().getInt("dense.vector.initial.size", 10));
  }

  @Override
  public MapVector zeroVector(int initialSize)
  {
    MapVector localVector = sparse ? new HashMapVector(initialSize) : new DenseMapVector(initialSize);
    MapVectorWritableComparable vector = new MapVectorWritableComparable(localVector);
    vector.setConf(getConf());
    return vector;
  }

  @Override
  public Configuration getConf()
  {
    return conf;
  }

  @Override
  public void setConf(Configuration config)
  {
    conf = config;
  }

}
