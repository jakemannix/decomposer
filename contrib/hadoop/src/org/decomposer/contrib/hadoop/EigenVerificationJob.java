package org.decomposer.contrib.hadoop;

import java.io.File;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.array.DenseMapVector;
import org.decomposer.util.FileUtils;

public class EigenVerificationJob extends BaseTool
{

  public static void main(String[] args) throws Exception
  {
    int ret = ToolRunner.run(new EigenVerificationJob(), args);
    System.exit(ret);
  }
  
  @Override
  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();
    conf.setBoolean("is.local", true);
    conf.set("job.name", System.currentTimeMillis() + "/");
    Properties configProps = loadJobProperties();
    
    HashMapDoubleMatrix originalVectorAsMatrix = FileUtils.deserialize(HashMapDoubleMatrix.class, 
                                                                       new File(configProps.getProperty("dense.vector.input.file")));
    DenseMapVector originalVector = (DenseMapVector)originalVectorAsMatrix.iterator().next().getValue();
    CacheUtils.addSerializableToCache(conf, originalVector, "inputVector");
    
    Job job = new Job(conf, "eigen-verification job");
    job.setJarByClass(EigenVerificationJob.class);
    job.setMapperClass(MatrixMultiplyMapper.class);
    job.setReducerClass(MatrixMultiplyReducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(DenseVectorWritableComparable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    String timestamp = new Date().toString().replace(' ', '_').replace(':', '_');
    
    Path outputPath = new Path(configProps.getProperty("dense.vector.output.path") + timestamp);
    
    FileInputFormat.addInputPath(job, new Path(configProps.getProperty("corpus.matrix.path")));
    FileOutputFormat.setOutputPath(job, outputPath);
    
    boolean successful = job.waitForCompletion(true);
    
    DenseMapVector outputVector = CacheUtils.readFirstValueFromSequenceFile(conf, 
                                                                            outputPath, 
                                                                            NullWritable.get(), 
                                                                            new DenseVectorWritableComparable()).getVector();
    double oneMinusCosAngle = originalVector.dot(outputVector) / Math.sqrt(originalVector.dot(originalVector) * outputVector.dot(outputVector));
    
    System.out.println("1 - cos(theta) = " + oneMinusCosAngle);
    // check!
    
    return successful ? 1 : -1;
  }
  
}
