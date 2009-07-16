package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;


public class TermCountingMapper extends Mapper<Object, Text, Text, IntWritable>
{

  private final static IntWritable one  = new IntWritable(1);
  private Text                     word = new Text();
  private Analyzer           analyzer;

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException
  {
    analyzer = new NGramAnalyzer(context.getConfiguration().getInt("ngram.maxValue", 2));
    TokenStream stream = analyzer.tokenStream("", new TextReader(value));
    Token token = new Token();
    while ((token = stream.next(token)) != null)
    {
      word.set(token.term());
      context.write(word, one);
    }
  }
}
