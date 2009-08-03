package org.decomposer.contrib.hadoop.job;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.io.CacheUtils;
import org.decomposer.contrib.hadoop.mapreduce.MatrixMultiplyMapper;
import org.decomposer.contrib.hadoop.mapreduce.MatrixMultiplyReducer;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.array.DenseMapVector;
import org.decomposer.math.vector.array.DenseMapVectorFactory;

public class MatrixMultiplyJob extends BaseTool
{

  public static void main(String[] args) throws Exception
  {
    int ret = ToolRunner.run(new MatrixMultiplyJob(), args);
    System.exit(ret);
  }
  
  public static DenseMapVector randomDenseMapVector(int numDimensions) 
  {
    DenseMapVector vector = (DenseMapVector) new DenseMapVectorFactory().zeroVector(numDimensions);
    Random rand = new Random(System.nanoTime() * numDimensions);
    for(int i=0; i<numDimensions; i++) vector.set(i, rand.nextDouble());
    vector.scale(1/vector.norm());
    return vector;
  }

  @Override
  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();
    conf.set("job.name", System.currentTimeMillis() + "/");
    Properties configProps = loadJobProperties();

    CacheUtils.addSerializableToCache(conf, randomDenseMapVector(100000), "inputVector");
    
    Job job = new Job(conf, "matrix multiply");
    job.setJarByClass(MatrixMultiplyJob.class);
    job.setMapperClass(MatrixMultiplyMapper.class);
    job.setReducerClass(MatrixMultiplyReducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(MapVectorWritableComparable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    String timestamp = new Date().toString().replace(' ', '_').replace(':', '_');
    
    FileInputFormat.addInputPath(job, new Path(configProps.getProperty("sparse.vector.output.path")));
    FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("dense.vector.output.path") + timestamp));
    
    return job.waitForCompletion(true) ? 1 : -1;
  }

}
