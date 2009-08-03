package org.decomposer.contrib.hadoop.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;

public class CacheUtils
{  
  public static String SERIALIZABLE_DIR = "serializableCache" + Path.SEPARATOR;
  
  public static void addSerializableToCache(Configuration job, Serializable serializable, String fileName)
  {
    try
    {
      Path workDir = new Path(SERIALIZABLE_DIR + job.get("job.name", ""));
      Path tempPath = new Path(workDir, fileName);
      //tempPath.getFileSystem(job).deleteOnExit(tempPath);
      job.set("serializables.directory", workDir.toUri().getPath());

      ObjectOutputStream objectStream = new ObjectOutputStream(tempPath.getFileSystem(job).create(tempPath));
      objectStream.writeObject(serializable);
      objectStream.close();

      DistributedCache.addCacheFile(new URI(tempPath.toUri().getPath() + "#" + tempPath.getName()), job);
    }
    catch (URISyntaxException e)
    {
      throw new RuntimeException(e);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Get the given Serializable from the distributed cache as an Object
   * 
   * @param conf the Configuration for the current job.
   * @return The Object that is read from cache
   */
  public static <T> T readSerializableFromCache(Configuration conf, String fileName, Class<T> clazz)
  {
    T t = null;
    ObjectInputStream stream = null;
    Path serializablePath = getSerializablePath(conf, fileName, conf.getBoolean("is.local", false));
    if (serializablePath == null)
      throw new IllegalStateException("No serialiable cache file found by the name of " + fileName);
    try
    {
      stream = new ObjectInputStream(new FileInputStream(serializablePath.toString()));
      Object obj = stream.readObject();
      t = (T)obj;
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
    }
    finally
    {
      if(stream != null) 
      {
        try { stream.close(); } catch(Exception e) {}
      }
    }
    return t;
  }
  
  public static void writeVectorToSequenceFile(MapVector vector, Path outputDir, Configuration conf) throws Exception
  {
    DoubleMatrix matrix = new HashMapDoubleMatrix(new HashMapVectorFactory());
    matrix.set(0, vector);
    writeMatrixToSequenceFile(matrix, outputDir, conf);
  }
  
  public static void writeMatrixToSequenceFile(DoubleMatrix matrix, Path outputDir, Configuration conf) throws Exception
  {
    FileSystem fs = FileSystem.get(conf);
    
    SequenceFile.Writer writer = new Writer(fs, 
                                            conf, 
                                            outputDir, 
                                            LongWritable.class, 
                                            MapVectorWritableComparable.class);
    MapVectorWritableComparable vectorWritable = new MapVectorWritableComparable(outputDir);
    LongWritable key = new LongWritable();
    for(Entry<Integer, MapVector> vector : matrix)
    {
      vectorWritable.setRow(vector.getKey());
      vectorWritable.setVector(vector.getValue());
      key.set(vector.getKey());
      writer.append(key, vectorWritable);
    } 
  }
  
  public static <K extends WritableComparable, V extends Writable> V readFirstValueFromSequenceFile(Configuration conf, 
                                                                                                    Path path, 
                                                                                                    Class<K> keyClass,
                                                                                                    Class<V> valueClass)
  {
    try
    {
      return readFirstValueFromSequenceFile(conf, path, keyClass.newInstance(), valueClass.newInstance());
    }
    catch (InstantiationException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (IllegalAccessException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  public static <K extends WritableComparable, V extends Writable> V readFirstValueFromSequenceFile(Configuration conf,
                                                                                                    Path path,
                                                                                                    K key,
                                                                                                    V value)
  {                                                   
    try
    {
      SequenceFile.Reader reader = new SequenceFile.Reader(path.getFileSystem(conf), path, conf);
      reader.next(key, value);
      reader.close();
      return value;
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  
  private static Path getSerializablePath(Configuration conf, String fileName, boolean isLocal)
  {
    Path serializablePath = null;
    if(isLocal)
    {
      serializablePath = new Path(SERIALIZABLE_DIR + conf.get("job.name", "") + fileName);
    }
    else
    {
      try
      {
        Path[] paths = DistributedCache.getLocalCacheFiles(conf);
        if(paths == null) throw new IOException("No paths on distributedCache getLocalCacheFiles call...");
        for(Path path : paths)
        {
          if(path.getName().equals(fileName))
            serializablePath = path;
        }
      }
      catch (IOException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return serializablePath;
  }
  
}
