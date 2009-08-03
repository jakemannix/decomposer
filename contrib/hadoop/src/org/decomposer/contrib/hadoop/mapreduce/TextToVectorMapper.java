package org.decomposer.contrib.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.decomposer.contrib.hadoop.math.DistributedVectorFactory;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
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

public class TextToVectorMapper extends Mapper<LongWritable, Text, LongWritable, MapVectorWritableComparable>
{
  ShingleFeatureExtractor featureExtractor;
  MapVectorWritableComparable outputValue;
  Text valueBuffer; 
  LongWritable keyBuffer;
  @Override
  public void setup(Context context) throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path dictionaryPath = new Path(context.getConfiguration().get("dictionary.output.path"));
    List<Feature> features = new ArrayList<Feature>();
    for(FileStatus status : fs.listStatus(dictionaryPath))
    {
      if(status.getPath().getName().startsWith("part"))
        features.addAll(loadFeatures(new Reader(fs, status.getPath(), conf)));
    }
  
    FeatureDictionary dictionary = new FeatureDictionary(features.toArray(new Feature[features.size()]));
    
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
                                                   },
                                                   context.getConfiguration());
    valueBuffer = new Text();
    keyBuffer = new LongWritable();
  }
  
  private List<Feature> loadFeatures(Reader reader) throws IOException
  {
    FeatureWritable featureWritable = new FeatureWritable();
    NullWritable nullWritable = NullWritable.get();
    List<Feature> features = new ArrayList<Feature>();
    while(reader.next(featureWritable, nullWritable))
    {
      Feature feature = new Feature();
      feature.name = featureWritable.name;
      feature.count = featureWritable.count;
      feature.id = featureWritable.id;
      features.add(feature);
    }
    reader.close();
    return features;
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    if(value.getLength() < 2 || value.find("</html>") >= 0)
    {
      append(value);
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
    outputValue = new MapVectorWritableComparable(featureExtractor.extract(valueBuffer), keyBuffer.get());
    valueBuffer.clear();
    context.write(keyBuffer, outputValue);
  }
  
  public static class ShingleFeatureExtractor implements FeatureExtractor
  {
    private FeatureDictionary phraseDictionary;
    private Analyzer analyzer; 
    private Idf idf;
    private double normalizationFactor;
    private VectorFactory vectorFactory;
    public ShingleFeatureExtractor(FeatureDictionary dictionary, 
                                   int n,
                                   Idf idf,
                                   Configuration config)
    {
      phraseDictionary = dictionary;
      analyzer = new NGramAnalyzer(n);
      this.idf = idf;
      vectorFactory = new DistributedVectorFactory(true);
      ((Configurable)vectorFactory).setConf(config);
      normalizationFactor = config.getFloat("input.text.vector.normalization.factor", 1);
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
          if(feature != null && feature.name.contains(" ")) vector.add(feature.id, idf.idf(phraseDictionary.getNumDocs(), feature.count));
        }
      }
      catch (IOException e)
      {
        
      }
      vector.scale(1/normalizationFactor);
      return vector;
    }

    @Override
    public MapVector extract(String inputText)
    {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException("Not yet supported");
    }
  }
  
  public static class FeatureDictionaryWritable extends FeatureDictionary implements Writable 
  {
    private static final long serialVersionUID = 1L;
    
    public FeatureDictionaryWritable(FeatureDictionary other)
    {
      this._featuresById = other._featuresById;
      this._featuresByName = other._featuresByName;
      this._maxId = other._maxId;
      this._numDocs = other._numDocs;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException
    {
      _numDocs = in.readInt();
      _maxId = in.readInt();
      int numFeatures = in.readInt();
      for(int i=0; i<numFeatures; i++)
      {
        Feature f = new Feature();
        f.name = in.readUTF();
        f.id = in.readInt();
        f.count = in.readInt();
        _featuresById.put(f.id, f);
        _featuresByName.put(f.name, f);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeInt(_numDocs);
      out.writeInt(_maxId);
      out.writeInt(_featuresById.size());
      for(Map.Entry<Integer, Feature> entry : _featuresById.entrySet())
      {
        Feature f = entry.getValue();
        out.writeUTF(f.name);
        out.writeInt(f.id);
        out.writeInt(f.count);
      }
    }
    
  }
  
  public static class FeatureWritable extends Feature implements Writable
  {
    private static final long serialVersionUID = 1L;

    @Override
    public void readFields(DataInput in) throws IOException
    {
      name = in.readUTF();
      id = in.readInt();
      count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeUTF(name);
      out.writeInt(id);
      out.writeInt(count);
    }
    
  }
}
