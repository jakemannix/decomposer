package org.decomposer.contrib.hadoop.math;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.decomposer.contrib.hadoop.Distributed;
import org.decomposer.contrib.hadoop.mapreduce.HebbianUpdateMapper;
import org.decomposer.contrib.hadoop.mapreduce.MatrixMultiplyMapper;
import org.decomposer.contrib.hadoop.mapreduce.MatrixMultiplyReducer;
import org.decomposer.contrib.hadoop.mapreduce.MatrixTransposeMapper;
import org.decomposer.contrib.hadoop.mapreduce.MatrixTransposeReducer;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.MapVector;

public class DistributedMatrix extends HashMapDoubleMatrix implements Distributed
{
  private static final Logger log = Logger.getLogger(DistributedMatrix.class.getName());
  
  private Path matrixPath;
  private Configuration conf;
  protected boolean localized;
  protected boolean useLocal;
  
  public DistributedMatrix(Path path)
  {
    this(path, true);
  }
  
  public DistributedMatrix(Path path, boolean useLocal)
  {
    super();
    matrixPath = path;
    _vectorFactory = new DistributedVectorFactory();
    localized = !useLocal;
    this.useLocal = useLocal;
  }
  
  public DistributedMatrix(Path path, DoubleMatrix other)
  {
    super(other);
    matrixPath = path;
    _vectorFactory = new DistributedVectorFactory();
    localized = false;
  }
  
  public DistributedMatrix(Path path, Configuration config)
  {
    this(path);
    setConf(config);
  }
  
  @Override
  public Path getPath()
  {
    return matrixPath;
  }
  
  public Configuration getConf()
  {
    return conf;
  }
  
  public void setConf(Configuration config)
  {
    conf = config;
    ((Configurable)_vectorFactory).setConf(config);
  }
  
  @Override
  public void localize() throws IOException
  {
    FileSystem.get(getConf()).copyToLocalFile(matrixPath, localPath());
    localized = true;
  }
  
  protected Path localPath()
  {
    return new Path(getConf().get("hadoop.tmp.dir") + "/matrixTmp/" + Math.abs(matrixPath.hashCode()));
  }
  
  public void distribute() throws IOException
  {
    FileSystem fs = FileSystem.get(getConf());
    
    /**
     *  We should really be keeping track of which rows live locally, and which are "out there", and distribute
     *  the ones that are only in memory when this is called (and then note that they are "out there" now).
     *  
     *  This is Not Yet Implemented, only distribute()'s if the <code>matrixPath</code> does not yet exist, and
     *  currently iterates over the in-memory vectors and writes them to the <code>FileSystem</code>
     */

    if(fs.exists(getPath())) return;
    

    SequenceFile.Writer writer = new Writer(fs, 
                                            getConf(), 
                                            getPath(), 
                                            LongWritable.class, 
                                            MapVectorWritableComparable.class);
    MapVectorWritableComparable vectorWritable = new MapVectorWritableComparable(getPath());
    LongWritable key = new LongWritable();
    Iterator<Entry<Integer, MapVector>> it = super.iterator();
    try
    { 
      while(it.hasNext())
      {
        Entry<Integer, MapVector> vector = it.next();
        vectorWritable.setRow(vector.getKey());
        vectorWritable.setVector(vector.getValue());
        key.set(vector.getKey());
        writer.append(key, vectorWritable);
      }
    }
    finally
    {
      writer.close();
    }
  }
  
  public MapVector timesSquared(MapVector inputVector) 
  {
    Path[] ioVectorPaths = getInputOutputPaths(inputVector);
    Path inputVectorPath = ioVectorPaths[0];
    Path outputVectorPath = ioVectorPaths[1];
    
    try
    {
      Job job = createMatrixMultiplyJob(inputVectorPath, outputVectorPath);
      job.waitForCompletion(true);
      MapVectorWritableComparable outputVector = new MapVectorWritableComparable(outputVectorPath);
      outputVector.setConf(getConf());
      outputVector.localize();
      return outputVector;
    }
    catch(IOException ioe) 
    {
      log.severe(ioe.getMessage());
    }
    catch(InterruptedException ie)
    {
      log.severe(ie.getMessage());
    }
    catch(ClassNotFoundException cfne)
    {
      log.severe(cfne.getMessage());
    }
    return null;
  }
  
  public DistributedMatrix transpose()
  {
    Path transposePath = new Path(matrixPath.getParent(), matrixPath.getName() + "_transpose");
    try
    {
      Job job = createTransposeJob(transposePath);
      job.waitForCompletion(true);
    }
    catch(IOException ioe)
    {
      ioe.printStackTrace();
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (ClassNotFoundException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return new DistributedMatrix(transposePath);
  }
  
  public MapVector hebbianPass(MapVector inputVector)
  {
    Path[] ioVectorPaths = getInputOutputPaths(inputVector);
    Path inputVectorPath = ioVectorPaths[0];
    Path outputVectorPath = ioVectorPaths[1];
    
    try
    {
      Job job = createHebbianUpdateJob(inputVectorPath, outputVectorPath);
      job.waitForCompletion(true);
      MapVectorWritableComparable outputVector = new MapVectorWritableComparable(outputVectorPath);
      outputVector.setConf(getConf());
      outputVector.localize();
      return outputVector;
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (ClassNotFoundException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }
  
  private Path[] getInputOutputPaths(MapVector inputVector)
  {
    Path inputVectorPath;
    if(inputVector == null)
    {
      inputVectorPath = new Path("DOES_NOT_EXIST");
    }
    else if(inputVector instanceof Distributed)
    {
      inputVectorPath = ((Distributed)inputVector).getPath();
      ((Distributed)inputVector).setConf(getConf());
    }
    else
    {
      inputVectorPath = new Path(matrixPath.getParent(), "/inputTemp/" + System.nanoTime());
      MapVectorWritableComparable distributedInputVector = new MapVectorWritableComparable(inputVector);
      distributedInputVector.setPath(inputVectorPath);
      distributedInputVector.setConf(getConf());
      try
      {
        distributedInputVector.distribute(); 
      }
      catch (IOException e)
      {
        log.severe(e.getMessage());
      }
    }
    getConf().set("inputVector", inputVectorPath.toString());
    
    Path outputVectorPath = new Path(matrixPath.getParent() + "/outputTemp/" + System.nanoTime());
    return new Path[] { inputVectorPath, outputVectorPath };
  }

  @Override
  public Iterator<Entry<Integer, MapVector>> iterator()
  {
    try
    {
      if(!localized) localize();
      FileSystem fs = useLocal ? FileSystem.getLocal(getConf()) : FileSystem.get(getConf());
      return new SequenceFileIterator(fs, useLocal ? localPath() : matrixPath, getConf());
    }
    catch(IOException ioe)
    {
      log.severe(ioe.getMessage());
    }
    return new Iterator<Entry<Integer, MapVector>>()
    {
      public boolean hasNext() { return false; }
      public Entry<Integer, MapVector> next() { return null; }
      public void remove() {}
    };
  }

  protected static class SequenceFileIterator implements Iterator<Entry<Integer, MapVector>>
  {
    SequenceFile.Reader reader;
    boolean hasReadNext;
    final MapVectorWritableComparable vector;
    final LongWritable row;
    boolean hasNext;
    SequenceFileIterator(FileSystem fs, Path localPath, Configuration conf) throws IOException
    {
      reader = new Reader(fs, localPath, conf);
      hasReadNext = false;
      vector = new MapVectorWritableComparable();
      row = new LongWritable();
      hasNext = false;
    }

    @Override
    public boolean hasNext()
    {
      advanceIfNecessary();
      return hasNext;
    }

    @Override
    public Entry<Integer, MapVector> next()
    {
      advanceIfNecessary();
      hasReadNext = false;
      return new Entry<Integer, MapVector>()
      {
        @Override
        public Integer getKey()
        {
          return (int)row.get();
        }

        @Override
        public MapVector getValue()
        {
          return vector.rawVector();
        }

        @Override
        public MapVector setValue(MapVector value)
        {
          // TODO Auto-generated method stub
          throw new UnsupportedOperationException("Not supported yet");
        }
      };
    }
    
    private void advanceIfNecessary()
    {
      if(!hasReadNext)
      {
        try
        {
          hasReadNext = true;
          hasNext = (reader.next(row, vector));
        }
        catch (IOException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
          hasReadNext = false;
          hasNext = false;
        }
      }
      if(!hasNext)
      {
        try
        {
          reader.close();
        }
        catch (IOException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

    @Override
    public void remove()
    {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException("Not supported yet");
    }
    
  }
  
  private Job createTransposeJob(Path transposePath) throws IOException
  {
    Job job = new Job(getConf(), "calculating transpose of matrix " + matrixPath + ", outputting to " + transposePath);
    
    job.setJarByClass(getClass());
    job.setMapperClass(MatrixTransposeMapper.class);
    job.setReducerClass(MatrixTransposeReducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(IntDoublePairWritableComparable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(MapVectorWritableComparable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    FileInputFormat.addInputPath(job, matrixPath);
    FileOutputFormat.setOutputPath(job, transposePath);
    
    return job;
  }

  
  protected Job createMatrixMultiplyJob(Path inputVectorPath, Path outputVectorPath) throws IOException
  {
    Job job = new Job(getConf(), "matrix multiplying " + inputVectorPath.toString() + " by " + matrixPath.toString() + " outputting to " + outputVectorPath.toString());

    job.setJarByClass(getClass());
    job.setMapperClass(MatrixMultiplyMapper.class);
    job.setReducerClass(MatrixMultiplyReducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(MapVectorWritableComparable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    getConf().set("inputVector", inputVectorPath.toString());
    FileInputFormat.addInputPath(job, matrixPath);
    FileOutputFormat.setOutputPath(job, outputVectorPath);
    
    return job;
  } 
  
  private Job createHebbianUpdateJob(Path inputVectorPath, Path outputVectorPath) throws IOException
  {
    Job job = new Job(getConf(), "hebbian pass using initial point " + inputVectorPath.toString() + " along matrix " + matrixPath.toString() + " outputting to " + outputVectorPath.toString());
    
    job.setJarByClass(getClass());
    job.setMapperClass(HebbianUpdateMapper.class);
    job.setReducerClass(MatrixMultiplyReducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(MapVectorWritableComparable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    getConf().set("inputVector", inputVectorPath.toString());
    FileInputFormat.addInputPath(job, matrixPath);
    FileOutputFormat.setOutputPath(job, outputVectorPath);
    
    return job;
  }
}
