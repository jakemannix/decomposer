package org.decomposer.contrib.hadoop.job;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.mapreduce.TextToVectorMapper;
import org.decomposer.contrib.hadoop.mapreduce.TextToVectorMapper.FeatureWritable;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;

public class TextToVectorJob extends BaseTool
{
  private static final Logger log = Logger.getLogger(TextToVectorJob.class.getName());
  
  private static class IdMapper extends Mapper<LongWritable, Text, LongWritable, Text>
  {
    private static LongWritable zero = new LongWritable(0);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
      context.write(zero, value);
    }
  }
  
  private static class FeatureDictionaryBuildingReducer extends Reducer<LongWritable, Text, FeatureWritable, NullWritable>
  {
    FeatureWritable featureWritable = new FeatureWritable();
    int maxFeatures;
    int numFeatures = 0;
    protected void setup(Context context) 
    {
      maxFeatures = context.getConfiguration().getInt("dictionary.max.features", 10000);
    }
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
      for(Text value : values)
      {
        if(numFeatures >= maxFeatures)
        {
          return;
        }
        String[] ngramAndCount = value.toString().split("\t");
        featureWritable.count = (int)((double)Double.valueOf(ngramAndCount[1]));
        featureWritable.id = numFeatures++;
        featureWritable.name = ngramAndCount[0];
        context.write(featureWritable, NullWritable.get());
      }
    }
  }
  
  public int run(String[] args) throws FileNotFoundException, IOException, InterruptedException, ClassNotFoundException
  {
    Configuration conf = getConf();
    conf.set("job.name", System.currentTimeMillis() + "/");
    Properties configProps = loadJobProperties();
    String timestamp = new Date().toString().replace(' ', '_').replace(':', '_');
    
    boolean verbose = Boolean.parseBoolean(configProps.getProperty("verbose"));
    conf.setInt("dictionary.max.features", Integer.parseInt(configProps.getProperty("dictionary.max.features")));
    conf.setFloat("input.text.vector.normalization.factor", Float.parseFloat(configProps.getProperty("input.text.vector.normalization.factor")));
    Job job = new Job(conf, "build dictionary");    
    job.setJarByClass(TextToVectorJob.class);
    job.setMapperClass(IdMapper.class);
    job.setReducerClass(FeatureDictionaryBuildingReducer.class);
    
    FileInputFormat.addInputPath(job, new Path(configProps.getProperty("ngram.sorted.path")));
    FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("dictionary.output.path") + timestamp));
    
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(FeatureWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(1);
    
    boolean successful = job.waitForCompletion(verbose);
    
    if(successful)
    {
      job = new Job(new Configuration(conf), "vectorize text");
      job.setJarByClass(TextToVectorJob.class);
      job.getConfiguration().set("dictionary.output.path", configProps.getProperty("dictionary.output.path") + timestamp);
      job.setMapperClass(TextToVectorMapper.class);
      job.getConfiguration().setInt("ngram.maxValue", Integer.parseInt(configProps.getProperty("ngram.maxValue"), 5));
      job.setNumReduceTasks(0);
      FileInputFormat.addInputPath(job, new Path(configProps.getProperty("text.input.path")));
      FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("sparse.vector.output.path") + timestamp));
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(MapVectorWritableComparable.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      successful = job.waitForCompletion(verbose);
    }
    return (successful ? 0 : 1);
  }

  
  public static void main(String[] args) throws Exception
  {
    int returnValue = ToolRunner.run(new TextToVectorJob(), args);
    System.exit(returnValue);
  }
  
}
