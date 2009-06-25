package org.decomposer.contrib.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.decomposer.contrib.hadoop.TextToSparseVectorMapper.FeatureDictionaryWritable;
import org.decomposer.nlp.extraction.FeatureDictionary;

public class TextToSparseVectorJob
{
  private static class IdMapper extends Mapper<LongWritable, Text, LongWritable, Text>
  {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
      super.map(key, value, context);
    }
    public void cleanup(Context context)
    {
      int i=0;
      i++;
    }
  }
  
  private static class ReverseComparator extends WritableComparator
  {
    public ReverseComparator()
    {
      super(LongWritable.class, true);
    }
    public int compare(WritableComparable a, WritableComparable b) 
    {
      return - super.compare(a, b);
    }
  }
  
  private static class FeatureDictionaryBuildingReducer extends Reducer<LongWritable, Text, FeatureDictionaryWritable, NullWritable>
  {
    FeatureDictionary dictionary = new FeatureDictionary();
    int maxFeatures;
    int numFeatures = 0;
    protected void setup(Context context) 
    {
      maxFeatures = context.getConfiguration().getInt("dictionary.max.features", 100000);
    }
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
    {
      for(Text value : values)
      {
        String ngram = value.toString().split("\t")[0];
        dictionary.updateFeature(ngram, 1.0);
        numFeatures++;
        if(numFeatures >= maxFeatures) return;
      }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException 
    {
      FeatureDictionaryWritable output = new FeatureDictionaryWritable();
      output.dictionary = dictionary;
      context.write(output, NullWritable.get());
    }
  }
  
  private static class BinarySequenceFileOutputFormat extends SequenceFileOutputFormat<FeatureDictionaryWritable, NullWritable>
  {
    @Override
    public RecordWriter<FeatureDictionaryWritable, NullWritable> getRecordWriter(TaskAttemptContext job) 
      throws IOException, InterruptedException
      {
        return super.getRecordWriter(job);
      }
  }
  
  private static class BinarySequenceFileInputFormat extends SequenceFileInputFormat<FeatureDictionaryWritable, NullWritable>
  {
    
  }
  
  public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException, ClassNotFoundException
  {
    Configuration conf = new Configuration();
    Properties configProps = new Properties();
    String timestamp = new Date().toString().replace(' ', '_').replace(':', '_');
    configProps.load(new FileInputStream(new File("../test.props")));

    boolean verbose = Boolean.parseBoolean(configProps.getProperty("verbose"));
    conf.setInt("dictionary.max.features", Integer.parseInt(configProps.getProperty("dictionary.max.features")));
    
    Job job = new Job(conf, "build dictionary");    
    job.setJarByClass(TextToSparseVectorJob.class);
    
    job.setMapperClass(IdMapper.class);
    job.setReducerClass(FeatureDictionaryBuildingReducer.class);
    FileInputFormat.addInputPath(job, new Path(configProps.getProperty("ngram.sorted.path")));
    
    FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("dictionary.output.path") + timestamp));
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(FeatureDictionaryWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(BinarySequenceFileOutputFormat.class);
    job.setNumReduceTasks(1);
    job.setSortComparatorClass(ReverseComparator.class);
    
    boolean failed = job.waitForCompletion(verbose);
    
    if(!failed)
    {
      job = new Job(conf, "vectorize text");
      job.setJarByClass(TextToSparseVectorJob.class);
      job.getConfiguration().set("dictionary.output.path", configProps.getProperty("dictionary.output.path") + timestamp);
      job.setMapperClass(TextToSparseVectorMapper.class);
      FileInputFormat.addInputPath(job, new Path(configProps.getProperty("text.input.path")));
      FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("sparse.vector.output.path") + timestamp));
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(SparseVectorWritableComparable.class);
    
      failed = job.waitForCompletion(verbose);
    }
    System.exit(failed ? 1 : 0);
  }
  
}
