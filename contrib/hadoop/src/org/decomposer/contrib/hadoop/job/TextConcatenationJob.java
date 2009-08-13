package org.decomposer.contrib.hadoop.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
    conf.setInt("header.lines.to.chop", Integer.parseInt(props.getProperty("header.lines.to.chop")));
    conf.setInt("footer.lines.to.chop", Integer.parseInt(props.getProperty("footer.lines.to.chop")));
    String timestamp = new Date().toString().replace(' ', '_').replace(':', '-');
    Job job = new Job(conf);
    job.setJarByClass(TextConcatenationJob.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    FileInputFormat.addInputPath(job, new Path(props.getProperty("text.input.path")));
    FileOutputFormat.setOutputPath(job, new Path(props.getProperty("text.output.path") + timestamp));
    return job.waitForCompletion(verbose) ? 1 : -1;
  }

  private static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>
  {
    
  }
  
  private static class MyReducer extends Reducer<LongWritable, Text, LongWritable, Text>
  {
    // look Ma, I'm not abstract!
  }
  
  private static class NewlineRemovingMapper extends Mapper<LongWritable, Text, LongWritable, Text>
  {
    private byte[] outputBytes;
    private int length;
    private Text concatenatedValues;
    private List<Integer> lineByteOffsets;
    private static byte[] NEXT = " ".getBytes();
    LongWritable outKey;
    protected void setup(Context context)
    {
      lineByteOffsets = new ArrayList<Integer>();
      lineByteOffsets.add(0);
      concatenatedValues = new Text();
      outputBytes = new byte[100];
      outKey = new LongWritable(0);
      length = 0;
    }
    
    private void append(byte[] newBytes)
    {
      if(newBytes.length > outputBytes.length - length)
      {
        byte[] bigger = new byte[2 * (newBytes.length + outputBytes.length)];
        System.arraycopy(outputBytes, 0, bigger, 0, length);
        outputBytes = bigger;
      }
      System.arraycopy(newBytes, 0, outputBytes, length, newBytes.length);
      length += newBytes.length;
    }
    
    public void map(LongWritable key, Text value, Context context) 
    {
      outKey.set(key.get());
      append(value.getBytes());
      lineByteOffsets.add(lineByteOffsets.get(lineByteOffsets.size() - 1) + value.getLength() + NEXT.length);
      append("\n".getBytes());
    }
    
    public void cleanup(Context context)
    {
      byte[] bytes = outputBytes;
      int footerLinesToChop = context.getConfiguration().getInt("footer.lines.to.chop", 0);
      int startFooterChop = bytes.length - 1;
      if(footerLinesToChop > 0 && lineByteOffsets.size() > footerLinesToChop)
      {
        startFooterChop = lineByteOffsets.get(lineByteOffsets.size() - footerLinesToChop);
      }
      int headerLinesToChop = context.getConfiguration().getInt("header.lines.to.chop", 0);
      int endHeaderChop = 0;
      if(headerLinesToChop > 0 && lineByteOffsets.size() > headerLinesToChop + footerLinesToChop)
      {
        endHeaderChop = lineByteOffsets.get(headerLinesToChop);
      }
      if(endHeaderChop > 0 || startFooterChop < bytes.length -1)
      {
        byte[] trimmedBytes = new byte[startFooterChop - endHeaderChop + 1];
        System.arraycopy(bytes, endHeaderChop, trimmedBytes, 0, trimmedBytes.length - 1);
        bytes = trimmedBytes;
      }
      if(bytes.length > 0) bytes[bytes.length - 1] = '\n';
      concatenatedValues.set(bytes);
      try
      {
        context.write(outKey, concatenatedValues);
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
      concatenatedValues.clear();
      length = 0;
    }
  }
  
}
