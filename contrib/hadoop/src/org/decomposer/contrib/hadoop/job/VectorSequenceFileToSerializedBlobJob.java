package org.decomposer.contrib.hadoop.job;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.DiskBufferedDoubleMatrix;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;

public class VectorSequenceFileToSerializedBlobJob extends BaseTool
{

  public static void main(String[] args) throws Exception
  {
    int retVal = ToolRunner.run(new VectorSequenceFileToSerializedBlobJob(), args);
    System.exit(retVal);
  }
  
  @Override
  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();
    conf.setBoolean("is.local", true);
    conf.set("job.name", System.currentTimeMillis() + "/");
    Properties configProps = loadJobProperties();
    conf.set("blob.output.path", configProps.getProperty("blob.output.path"));
    
    File outputPath = new File(configProps.getProperty("blob.output.path"));
    if(!outputPath.exists()) outputPath.mkdir();
    
    conf.setInt("output.block.size", Integer.parseInt(configProps.getProperty("output.block.size")));
    Job job = new Job(conf, "");
    job.setJarByClass(VectorSequenceFileToSerializedBlobJob.class);
    job.setMapperClass(BlobMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(configProps.getProperty("sparse.vector.sequence.file.input.path")));
    FileOutputFormat.setOutputPath(job, new Path("/tmp/nothingToWriteHere" + System.nanoTime()));
    
    boolean successful = job.waitForCompletion(true);
    return successful ? 1 : -1;
  }

  public static class BlobMapper extends Mapper<LongWritable, MapVectorWritableComparable, NullWritable, NullWritable>
  {
    File outputPath;
    DoubleMatrix matrix;
    int blockSize;
    
    @Override
    public void setup(Context context)
    {
      outputPath = new File(context.getConfiguration().get("blob.output.path"));
      blockSize = context.getConfiguration().getInt("output.block.size", 1000);
      matrix = new HashMapDoubleMatrix(new HashMapVectorFactory());
    }
    @Override
    public void map(LongWritable key, MapVectorWritableComparable value, Context context) 
    {
      if(matrix.numRows() >= blockSize) flush();
      matrix.set((int)key.get(), value);
    }
    
    public void cleanup(Context context)
    {
      flush();
    }
    
    private void flush()
    {
      try
      {
        DiskBufferedDoubleMatrix.persistChunk(outputPath, matrix, true);
        matrix = new HashMapDoubleMatrix(new HashMapVectorFactory());
      }
      catch (FileNotFoundException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      catch (IOException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
    }
  }
}
