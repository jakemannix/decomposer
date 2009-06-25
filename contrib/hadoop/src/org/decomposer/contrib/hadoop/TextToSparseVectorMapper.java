package org.decomposer.contrib.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.nlp.extraction.DelimitedDictionaryFeatureExtractor;
import org.decomposer.nlp.extraction.FeatureDictionary;
import org.decomposer.nlp.extraction.FeatureExtractor;

public class TextToSparseVectorMapper extends Mapper<LongWritable, Text, LongWritable, SparseVectorWritableComparable>
{
  FeatureExtractor featureExtractor;
  SparseVectorWritableComparable outputValue = new SparseVectorWritableComparable();

  @Override
  public void setup(Context context) throws IOException, InterruptedException
  {
    FSDataInputStream input = CacheUtils.getLocalCacheFile(context.getConfiguration(), context.getConfiguration().get("dictionary.output.path"));
    FeatureDictionaryWritable d = new FeatureDictionaryWritable();
    d.readFields(input);
    featureExtractor = new DelimitedDictionaryFeatureExtractor(d.dictionary);
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    outputValue.setVector((ImmutableSparseMapVector) featureExtractor.extract(value.toString()));
    outputValue.setRow(key.get());
    context.write(key, outputValue);
  }
  
  public static class FeatureDictionaryWritable implements Writable
  {
    FeatureDictionary dictionary;
    
    
    @Override
    public void readFields(DataInput in) throws IOException
    {
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      ObjectInputStream ois = new ObjectInputStream(bais);
      try
      {
        dictionary = (FeatureDictionary)ois.readObject();
      }
      catch (ClassNotFoundException e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(dictionary);
      byte[] bytes = baos.toByteArray();
      out.write(bytes.length);
      out.write(bytes);
    }
    
  }
}
