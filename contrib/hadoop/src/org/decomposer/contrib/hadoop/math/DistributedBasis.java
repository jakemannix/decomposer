package org.decomposer.contrib.hadoop.math;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.decomposer.contrib.hadoop.mapreduce.BasisOrthogonalizeMapper;
import org.decomposer.contrib.hadoop.mapreduce.BasisOrthogonalizeReducer;

public class DistributedBasis extends Configured
{
  protected Path basePath;
  protected Path[] vectorPaths;
  
  public DistributedBasis(Path basePath)
  {
    this.basePath = basePath;
  }
  
  public boolean orthogonalize(Path inputVectorPath, Path outputVectorPath) throws IOException, InterruptedException, ClassNotFoundException
  {
    Job orthogonalizeJob = createOrthogonalizeJob(inputVectorPath, outputVectorPath, getVectorPaths());
    return orthogonalizeJob.waitForCompletion(true);
  }
  
  protected Job createOrthogonalizeJob(Path vectorPath, Path outputPath, Path[] basisVectorPaths) throws IOException
  {
    Job job = new Job(getConf(), "orthogonalize " + vectorPath.toString() + " against basis found in " + basePath.toString());
    for(Path basisVectorPath : basisVectorPaths) FileInputFormat.addInputPath(job, basisVectorPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(DistributedBasis.class);
    job.setMapperClass(BasisOrthogonalizeMapper.class);
    job.setReducerClass(BasisOrthogonalizeReducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(MapVectorWritableComparable.class);
    
    getConf().set("inputVector", vectorPath.toString());
    return job;
  }

  protected Path[] getVectorPaths() throws IOException
  {
    if(vectorPaths == null)
    {
      FileSystem fs = FileSystem.get(getConf());
      List<Path> paths = new ArrayList<Path>();
      for(FileStatus fileStatus : fs.listStatus(basePath, new PathFilter()
      {
        @Override
        public boolean accept(Path p)
        {
          try { return Integer.parseInt(p.getName()) >= 0; }
          catch(NumberFormatException nfe) { return false; }        
        }
      })) paths.add(fileStatus.getPath());
      vectorPaths = paths.toArray(new Path[paths.size()]);
    }
    return vectorPaths;
  }
}
