package org.decomposer.contrib.hadoop.job;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.phrases.IntSumReducer;
import org.decomposer.contrib.hadoop.phrases.MLReducer;
import org.decomposer.contrib.hadoop.phrases.SubNGramMapper;
import org.decomposer.contrib.hadoop.phrases.SubNGramReducer;
import org.decomposer.contrib.hadoop.phrases.NGramCountingMapper;
import org.decomposer.contrib.hadoop.phrases.TextAndInt;


public class PhraseExtractorJob extends BaseTool
{  
  public static abstract class InvertingMapper<T extends WritableComparable> extends Mapper<Object, Text, T, Text>
  {
    public abstract T getResult();
    public abstract void parse(String input);
    public boolean valid(Context context) { return true; }
    
    private Text output = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] subStrings = value.toString().split("\t");
      parse(subStrings[1]);
      output.set(subStrings[0].trim());
      if(valid(context)) context.write(getResult(), output);
    }
  }
  
  public static class FilterAndReverseIntMapper extends InvertingMapper<IntWritable>
  {
    IntWritable result = new IntWritable();
    @Override
    public IntWritable getResult() { return result; }
    @Override
    public void parse(String input) { result.set(Integer.parseInt(input)); }
    @Override
    public boolean valid(Context context)
    {
      return result.get() > context.getConfiguration().getInt("filter.minValue", -1);
    }
  }
  
  public static class InvertingFloatMapper extends InvertingMapper<FloatWritable>
  {
    FloatWritable result = new FloatWritable();
    @Override
    public FloatWritable getResult() { return result; }
    @Override
    public void parse(String input) { result.set(Float.parseFloat(input)); }
  }
  
  public static class InvertingReducer<A, B> extends Reducer<A, B, B, A>
  {
    @Override
    public void reduce(A key, Iterable<B> values, Context context) throws IOException, InterruptedException
    {
      for(B value : values) context.write(value, key);
    }
  }
  
  public static class InverseIntReducer extends InvertingReducer<IntWritable, Text> {}
  
  public static class InverseFloatReducer extends InvertingReducer<FloatWritable, Text> {}
  
  public static abstract class ReverseWritableComparator extends WritableComparator
  {
    protected ReverseWritableComparator(Class<? extends WritableComparable> keyClass)
    {
      super(keyClass, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b)
    {
      return super.compare(b, a);
    }
  }
  
  public static class ReverseFloatWritableComparator extends ReverseWritableComparator
  {
    public ReverseFloatWritableComparator()
    {
      super(FloatWritable.class);
    }
  }
  
  public static class ReverseIntWritableComparator extends ReverseWritableComparator
  {
    public ReverseIntWritableComparator()
    {
      super(IntWritable.class);
    }
    
  }
  
  public static enum PhrazerCounterTypes
  {
    NUM_WRONG_SORTS,
    NUM_SUB_NGRAM_MAPPERS,
    NUM_SUB_NGRAM_REDUCERS,
    NUM_REDUCER_VALUES
  }
  
  public static final class TextIdentityMapper extends Mapper<Object, Text, Text, Text>
  {
    private Text outKey = new Text();
    private Text outValue = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] keyAndValue = value.toString().split("\t");
      outKey.set(keyAndValue[0]);
      outValue.set("{"+keyAndValue[1]+":"+keyAndValue[2]+"}");
      context.write(outKey, outValue);
    }
  }
  

  @Override
  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();
    Properties configProps = loadJobProperties();
    String timestamp = new Date().toString().replace(' ', '_').replace(':', '_');
    conf.setInt("filter.minValue", Integer.parseInt(configProps.getProperty("filter.minValue")));
    conf.setInt("ngram.maxValue", Integer.parseInt(configProps.getProperty("ngram.maxValue")));
    conf.setBoolean("input.isHtml", Boolean.parseBoolean(configProps.getProperty("input.isHtml")));
    boolean verbose = Boolean.parseBoolean(configProps.getProperty("verbose"));
    
    /**
     * First job: tokenize and shinglize the input text, and count the occurrences of each <N>gram, up to <N> = ngram.maxValue
     */
    
    Job job = new Job(conf, "word count");
    job.setJarByClass(PhraseExtractorJob.class);
    job.setMapperClass(NGramCountingMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(configProps.getProperty("input.path")));
    FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("ngram.count.path") + timestamp));
    boolean failed = !job.waitForCompletion(verbose);
    
    long numNGrams = job.getCounters().findCounter(PhrazerCounterTypes.NUM_REDUCER_VALUES).getValue();
    conf.set("ngrams.count", String.valueOf(numNGrams));
    
    /**
     * Second job: sort by ngram counts, filtering out all ngrams occurring less than filter.minValue times
     */
    
    if(!failed)
    {
      job = new Job(new Configuration(conf), "filter and sort");
      job.setJarByClass(PhraseExtractorJob.class);
      job.setMapperClass(FilterAndReverseIntMapper.class);
      job.setReducerClass(InverseIntReducer.class);
      job.setSortComparatorClass(ReverseIntWritableComparator.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(configProps.getProperty("ngram.count.path") + timestamp));
      FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("ngram.sorted.path") + timestamp));
      failed = !job.waitForCompletion(verbose);
    }
    
    /**
     * If ngram.maxValue = 1, we're just doing "WordCount", and so we're done, just copy to the output dir
     */
    
    if(!failed && conf.getInt("ngram.maxValue", 2) == 1)
    {
      Path outputPath = new Path(configProps.getProperty("ngram.final.output.path") + timestamp);
      FileSystem fs = FileSystem.get(conf);
      fs.rename(new Path(configProps.getProperty("ngram.sorted.path") + timestamp), outputPath);
      return failed ? -1 : 1;
    }
    
    /**
     * Third job: the sorted self-join: outputs key-value pairs such as 
     *   "the quick brown" => {quick brown:12345},  
     *   "the quick brown" => {the:9283744},
     *   "the quick brown" => {the quick brown:12}
     *   
     *   note: these three would have arrived at *different* reducers - the same reducer might output
     *   
     *   "the quick brown" => {quick brown:12345}
     *   "quick brown fox" => {quick brown:12345}
     *   "when quick brown" => {quick brown:12345}
     */
    
    if(!failed)
    {
      job = new Job(new Configuration(conf), "self-merge");
      job.setJarByClass(PhraseExtractorJob.class);
      job.setMapperClass(SubNGramMapper.class);
      job.setReducerClass(SubNGramReducer.class);
      job.setPartitionerClass(SubNGramMapper.TextAndIntPartitioner.class);
      job.setGroupingComparatorClass(SubNGramReducer.GroupingComparator.class);
      job.setMapOutputKeyClass(TextAndInt.class);
      job.setMapOutputValueClass(TextAndInt.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(TextAndInt.class);
      FileInputFormat.addInputPath(job, new Path(configProps.getProperty("ngram.sorted.path") + timestamp));
      FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("ngram.joined.path") + timestamp));
      failed = !job.waitForCompletion(verbose);
    }
    
    /**
     * Fourth job: collect together key-value pairs from the previous job, grouped by key, and compute the 
     * ML probability of the ngram (the key), and output this.
     */
    
    if(!failed)
    {
      job = new Job(new Configuration(conf), "combine");
      job.setJarByClass(PhraseExtractorJob.class);
      job.setMapperClass(TextIdentityMapper.class);
      job.setReducerClass(MLReducer.class);         // This reducer is where the magic probabilities get computed.
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);
      FileInputFormat.addInputPath(job, new Path(configProps.getProperty("ngram.joined.path") + timestamp));
      FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("ngram.output.path") + timestamp));
      failed = !job.waitForCompletion(verbose);
    }
    
    /**
     * 5th job: sort descending by value
     */
    
    if(!failed)
    {
      job = new Job(new Configuration(conf), "final sort");
      job.setJarByClass(PhraseExtractorJob.class);
      job.setMapperClass(InvertingFloatMapper.class);
      job.setReducerClass(InverseFloatReducer.class);
      job.setSortComparatorClass(ReverseFloatWritableComparator.class);
      job.setMapOutputKeyClass(FloatWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);
      FileInputFormat.addInputPath(job, new Path(configProps.getProperty("ngram.output.path") + timestamp));
      FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("ngram.final.output.path") + timestamp));
      failed = !job.waitForCompletion(verbose);
    }
    
    return failed ? -1 : 1;
  }

  public static void main(String[] args) throws Exception
  {
    int retVal = ToolRunner.run(new PhraseExtractorJob(), args);
    System.exit(retVal);
  }
}
