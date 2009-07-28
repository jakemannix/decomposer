package org.decomposer.contrib.hadoop.job;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;

public class TextConcatenationJob extends BaseTool
{

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception
  {
    int retVal = ToolRunner.run(new TextConcatenationJob(), args);
    System.exit(retVal);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();
    Properties props = loadJobProperties();
    boolean verbose = Boolean.parseBoolean(props.getProperty("verbose", "false"));
    String timestamp = new Date().toString().replace(' ', '_').replace(':', '-');
    Job job = new Job(conf);
    job.setJarByClass(TextConcatenationJob.class);
    job.setMapperClass(NewlineRemovingMapper.class);
    job.setReducerClass(MyReducer.class);
    FileInputFormat.addInputPath(job, new Path(props.getProperty("text.input.path")));
    FileOutputFormat.setOutputPath(job, new Path(props.getProperty("text.output.path") + timestamp));
    return job.waitForCompletion(verbose) ? 1 : -1;
  }

  private static class MyReducer extends Reducer<LongWritable, Text, NullWritable, Text>
  {
    // look Ma, I'm not abstract!
  }
  
  private static class NewlineRemovingMapper extends Mapper<LongWritable, Text, LongWritable, Text>
  {
    private Text concatenatedValues = new Text();
    
    public void map(LongWritable key, Text value, Context context) 
    {
      concatenatedValues.append(value.getBytes(), 0, value.getLength());
    }
    
    public void cleanup(Context context)
    {
      byte[] bytes = concatenatedValues.getBytes();
      for(int i=0; i<bytes.length; i++)
      {
        if(bytes[i] == '\n' || bytes[i] == '\r') bytes[i] = ' ';
      }
      try
      {
        context.write(new LongWritable(0), concatenatedValues);
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
    }
  }
  
}
