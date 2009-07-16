package org.decomposer.contrib.hadoop.phrases;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SecondarySort
{
  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    Properties configProps = new Properties();
    String timestamp = new Date().toString().replace(' ', '_').replace(':', '_');
    configProps.load(new FileInputStream(new File("test.props")));
    conf.setInt("filter.minValue", Integer.parseInt(configProps.getProperty("filter.minValue")));
    conf.setInt("ngram.maxValue", Integer.parseInt(configProps.getProperty("ngram.maxValue")));
    boolean verbose = Boolean.parseBoolean(configProps.getProperty("verbose"));
    
    Job job = new Job(conf, "test custom datatypes");
    
    job.setJarByClass(SecondarySort.class);
    
    job.setMapperClass(SubNGramMapper.class);
    job.setReducerClass(SubNGramReducer.class);
    
    job.setPartitionerClass(SubNGramMapper.TextAndIntPartitioner.class);
    job.setGroupingComparatorClass(SubNGramReducer.GroupingComparator.class);
    
    job.setMapOutputKeyClass(TextAndInt.class);
    job.setMapOutputValueClass(TextAndInt.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextAndInt.class);
    
    FileInputFormat.addInputPath(job, new Path(configProps.getProperty("ngram.sorted.path")));
    FileOutputFormat.setOutputPath(job, new Path(configProps.getProperty("ngram.joined.path") + timestamp));
    
    System.exit(job.waitForCompletion(verbose) ? 0 : 1);
  }
}
