package org.decomposer.contrib.hadoop.math;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.decomposer.contrib.hadoop.Distributed;
import org.decomposer.contrib.hadoop.io.CacheUtils;
import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.array.DenseMapVector;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVector;

public class MapVectorWritableComparable implements WritableComparable<MapVectorWritableComparable>, MapVector, Distributed
{
  protected MapVector mapVector;
  protected long rowNum;
  
  protected Path path;
  protected Configuration conf;
  
  public MapVectorWritableComparable(MapVector vector, long row)
  {
    mapVector = vector;
    rowNum = row;
  }
  
  public MapVectorWritableComparable(MapVector vector)
  {
    this(vector, -1L);
  }
  
  public MapVectorWritableComparable(Path path, long row)
  {
    this.path = path;
    rowNum = row;
  }
  
  public MapVectorWritableComparable()
  {
    rowNum = -1L;
  }
  
  public MapVectorWritableComparable(Path path)
  {
    this(path, -1L);
  }
  
  MapVector rawVector() { return mapVector; }
  
  public void setPath(Path path) { this.path = path; }
  public Path getPath() { return path; }
  
  public void setConf(Configuration config) { conf = config; }  
  public Configuration getConf() { return conf; }
  
  public void localize() throws IOException
  {
    LongWritable key = new LongWritable();
    long savedRowNum = rowNum;
    FileSystem fs = path.getFileSystem(getConf());
    if(!fs.exists(getPath()))
    {
      mapVector = null;
      return;
    }
    try
    {
      for(FileStatus status : fs.listStatus(getPath(), new PathFilter()
      {
        @Override
        public boolean accept(Path path)
        {
          return !path.getName().startsWith(".") && !path.getName().startsWith("_");
        }
      }))
      {
        Path subPath = status.getPath();  
      
        SequenceFile.Reader reader = new SequenceFile.Reader(subPath.getFileSystem(conf), subPath, conf);
        while(reader.next(key, this))
        {
          if(key.get() == savedRowNum) break;
        }
        reader.close();
        if(key.get() == savedRowNum) break;
      }
    }
    finally
    {
      if(savedRowNum != rowNum)
      {
        mapVector = null;
        rowNum = savedRowNum;
      }
    }
  }
  
  public void distribute() throws IOException
  {
    FileSystem fs = FileSystem.get(conf);
    
    if(fs.exists(getPath())) return;
    
    SequenceFile.Writer writer = new Writer(fs, 
                                            getConf(), 
                                            getPath(), 
                                            LongWritable.class, 
                                            MapVectorWritableComparable.class);
    LongWritable key = new LongWritable(rowNum);
    writer.append(key, this);
    writer.close();
  }

  public void setRow(long row) { rowNum = row; }
  
  public void setVector(MapVector vector) { mapVector = vector; }
  
  @Override
  public void readFields(DataInput in) throws IOException
  {
    String vectorClassName = in.readUTF();
    rowNum = in.readLong();
    int size = in.readInt();
    if(DenseMapVector.class.getName().equals(vectorClassName))
    {
      double[] values = new double[size];
      for(int i=0; i<size; i++)
        values[i] = in.readDouble();
      mapVector = new DenseMapVector(values);
    }
    else
    {
      int[] indices = new int[size];
      double[] values = new double[size];
      for(int i=0; i<size; i++)
      {
        indices[i] = in.readInt();
        values[i] = in.readDouble();
      }
      mapVector = new ImmutableSparseMapVector(indices, values);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    if(mapVector instanceof DenseMapVector)
    {
      out.writeUTF(DenseMapVector.class.getName());
      out.writeLong(rowNum);
      int size = mapVector.maxDimension();
      out.writeInt(size);
      for(IntDoublePair pair : mapVector)
        out.writeDouble(pair.getDouble());
    }
    else
    {
      ImmutableSparseMapVector sparseMapVector = new ImmutableSparseMapVector(mapVector);
      out.writeUTF(ImmutableSparseMapVector.class.getName());
      out.writeLong(rowNum);
      int size = sparseMapVector.numNonZeroEntries();
      out.writeInt(size);
      for(IntDoublePair pair : sparseMapVector)
      {
        out.writeInt(pair.getInt());
        out.writeDouble(pair.getDouble());
      }
    }
  }

  public MapVectorWritableComparable clone()
  {
    return new MapVectorWritableComparable(mapVector.clone(), rowNum);
  }
  
  @Override
  public int compareTo(MapVectorWritableComparable o)
  {
    if(rowNum == o.rowNum) return 0;
    return rowNum > o.rowNum ? 1 : -1;
  }

  @Override
  public void add(int index, double toBeAdded)
  {
    mapVector.add(index, toBeAdded);
  }

  @Override
  public double dot(MapVector vector)
  {
    return mapVector.dot(vector);
  }

  @Override
  public double get(int index)
  {
    return mapVector.get(index);
  }

  @Override
  public Iterator<IntDoublePair> iterator()
  {
    return mapVector.iterator();
  }

  @Override
  public int maxDimension()
  {
    return mapVector.maxDimension();
  }

  @Override
  public double norm()
  {
    return mapVector != null ? mapVector.norm() : 0;
  }

  @Override
  public double normSquared()
  {
    return mapVector != null ? mapVector.normSquared() : 0;
  }

  @Override
  public int numNonZeroEntries()
  {
    return mapVector.numNonZeroEntries();
  }

  @Override
  public MapVector plus(MapVector vector)
  {
    if(mapVector == null) 
    {
      if(vector instanceof DenseMapVector)
      {
        mapVector = new DenseMapVector((DenseMapVector)vector);
      }
      else
      {
        mapVector = new HashMapVector(vector);
      }
      return this;
    }
    mapVector.plus(vector);
    return this;
  }

  @Override
  public MapVector plus(MapVector vector, double scale)
  {
    mapVector.plus(vector, scale);
    return this;
  }

  @Override
  public MapVector scale(double scale)
  {
    mapVector.scale(scale);
    return this;
  }

  @Override
  public void set(int index, double value)
  {
    mapVector.set(index, value);
  }

}
