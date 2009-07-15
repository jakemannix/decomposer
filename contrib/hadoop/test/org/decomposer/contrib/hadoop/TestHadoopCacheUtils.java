package org.decomposer.contrib.hadoop;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import junit.framework.TestCase;

public class TestHadoopCacheUtils extends TestCase
{

  private static Path TEST_ROOT_DIR = new Path(System.getProperty("test.build.data","/tmp"));
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  
  static 
  {
    try 
    {
      localFs = FileSystem.getLocal(conf);
    } 
    catch (IOException io) 
    {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  public Path writeFile(String name, String data) throws IOException 
  {
    Path file = new Path(TEST_ROOT_DIR + "/" + name);
    localFs.delete(file, false);
    DataOutputStream f = localFs.create(file);
    f.write(data.getBytes());
    f.close();
    return file;
  }

  public String readFile(String name) throws IOException 
  {
    DataInputStream f = localFs.open(new Path(TEST_ROOT_DIR + "/" + name));
    BufferedReader b = new BufferedReader(new InputStreamReader(f));
    StringBuilder result = new StringBuilder();
    String line = b.readLine();
    while (line != null) 
    {
     result.append(line);
     result.append('\n');
     line = b.readLine();
    }
    b.close();
    return result.toString();
  }

  
  public TestHadoopCacheUtils(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    super.setUp();
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }
  
  public void testCacheUtils() throws Exception
  {
    Configuration conf = new Configuration();
    localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
    localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);  
    writeFile("in/test1", "this file has only a few lines on it\nbut they are useful\nto some people, i think.");

    Job job = new Job(conf, "test");
    job.setJarByClass(TestHadoopCacheUtils.class);
    job.setMapperClass(TestMapper.class);
    job.setReducerClass(TestReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);    
    FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
    FileOutputFormat.setOutputPath(job, new Path(TEST_ROOT_DIR + "/out"));
    
    assertTrue(job.waitForCompletion(true));
    String out = readFile("out/part-r-00000");
    System.out.println(out);
  }

  static class TestMapper extends Mapper<LongWritable, Text, Text, IntWritable>
  {
    public void setup(Context context)
    {
      String s = "this string has useful information";
      CacheUtils.addSerializableToCache(context.getConfiguration(), s, "idfMap");
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] tokens = value.toString().split(" ");
      for(String token : tokens) context.write(new Text(token), new IntWritable(1));
    }
  }
  
  static class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    Set<String> stringsToKeep;
    @Override
    public void setup(Context context)
    {
      stringsToKeep = new HashSet<String>();
      String cachedString = CacheUtils.readSerializableFromCache(context.getConfiguration(), "idfMap", String.class);
      for(String subString : cachedString.split(" "))
      {
        stringsToKeep.add(subString);
      }
    }
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
      int count = 0;
      if(stringsToKeep.contains(key.toString()))
      {
        for(IntWritable value : values) count += value.get();
      }
      context.write(key, new IntWritable(count));
    }
  }
}
