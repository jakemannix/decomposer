package org.decomposer.contrib.hadoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.decomposer.contrib.hadoop.io.CacheUtils;
import org.decomposer.contrib.hadoop.io.SparseVectorWritableComparable;
import org.decomposer.contrib.hadoop.phrases.NGramAnalyzer;
import org.decomposer.contrib.hadoop.phrases.TextReader;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;
import org.decomposer.nlp.extraction.FeatureDictionary;
import org.decomposer.nlp.extraction.FeatureExtractor;
import org.decomposer.nlp.extraction.Idf;
import org.decomposer.nlp.extraction.FeatureDictionary.Feature;

public class TextToSparseVectorMapper extends Mapper<LongWritable, Text, LongWritable, SparseVectorWritableComparable>
{
  ShingleFeatureExtractor featureExtractor;
  SparseVectorWritableComparable outputValue = new SparseVectorWritableComparable();
  Text valueBuffer; 
  LongWritable keyBuffer;
  @Override
  public void setup(Context context) throws IOException, InterruptedException
  {
    FeatureDictionary dictionary = CacheUtils.readSerializableFromCache(context.getConfiguration(), 
                                                                        "dictionary", 
                                                                        FeatureDictionary.class);
  //  FSDataInputStream input = CacheUtils.getLocalCacheFile(context.getConfiguration(), context.getConfiguration().get("dictionary.output.path"));
  //  FeatureDictionaryWritable d = new FeatureDictionaryWritable();
  //  d.readFields(input);
    featureExtractor = new ShingleFeatureExtractor(dictionary, 
                                                   context.getConfiguration().getInt("ngram.maxValue", 5),
                                                   new Idf() 
                                                   {
                                                     @Override
                                                     public double idf(int numDocs, int count)
                                                     {
                                                       return Math.log((numDocs + 1) / (count + 1));
                                                     }
                                                   });
    valueBuffer = new Text();
    keyBuffer = new LongWritable();
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    if(value.getLength() < 2)
    {
      if(valueBuffer.getLength() > 0)
        flushBuffer(context);
    }
    else
    {
      if(valueBuffer.getLength() == 0)
        keyBuffer.set(key.get());
      append(value);
    }
  }
  
  @Override
  public void cleanup(Context context)
  {
    try
    {
      flushBuffer(context);
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
  
  private void append(Text value)
  {
    valueBuffer.append(value.getBytes(), 0, value.getLength());
  }
  
  private void flushBuffer(Context context) throws IOException, InterruptedException
  {
    outputValue.setVector((ImmutableSparseMapVector) featureExtractor.extract(valueBuffer));
    outputValue.setRow(keyBuffer.get());
    context.write(keyBuffer, outputValue);
  }
  
  public static class ShingleFeatureExtractor implements FeatureExtractor
  {
    private FeatureDictionary phraseDictionary;
    private Analyzer analyzer; 
    private Idf idf;
    private VectorFactory vectorFactory = new HashMapVectorFactory();
    public ShingleFeatureExtractor(FeatureDictionary dictionary, 
                                   int n,
                                   Idf idf)
    {
      phraseDictionary = dictionary;
      analyzer = new NGramAnalyzer(n);
      this.idf = idf;
    }
    
    public MapVector extract(Text input)
    {
      MapVector vector = vectorFactory.zeroVector(Math.min(10, input.getLength()/10));
      Token token = new Token();
      TokenStream stream = analyzer.tokenStream("dummy", new TextReader(input));
      try
      {
        while((token = stream.next(token)) != null)
        {
          Feature feature = phraseDictionary.getFeature(token.term());
          if(feature != null) vector.add(feature.id, idf.idf(phraseDictionary.getNumDocs(), feature.count));
        }
      }
      catch (IOException e)
      {
        
      }
      return new ImmutableSparseMapVector(vector);
    }

    @Override
    public MapVector extract(String inputText)
    {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException("Not yet supported");
    }
  }
  
  public static class FeatureDictionaryWritable implements Writable
  {
    public FeatureDictionary dictionary;
    
    
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
