package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
   * Takes in input text file with lines as follows:
   *   
   *   a class unselected      2
   *   a class  4
   *   a  90
   *   
   * These will be mapped to the following key value pairs:
   * a class unselected 2
   *   {{text : a, orig_n : 3} => {text : a class unselected, count : 2}}
   *   {{text : class, orig_n : 3} => {text : a class unselected, count : 2}}
   *   {{text : unselected, orig_n : 3} => {text : a class unselected, count : 2}}
   *   {{a class, orig_n : 3} => {text : a class unselected, count : 2}}
   *   {{class unselected, orig_n : 3} => {text : a class unselected, count : 2}}
   *   {{a class unselected, orig_n : 3} => {text : a class unselected, count : 2}}
   *   
   *   and
   *   
   * a class  4
   *   {{a, orig_n : 2} => {text : a class, count : 4}}
   *   {{class, orig_n : 2} => {text : a class, count : 4}}
   *   {{a class, orig_n : 2} => {text : a class, count : 4}}
   *   
   *   and
   *   
   * a  90
   *   {{a, orig_n : 1} => {text : a, count : 90}}
   *   
   * Thus the reducer for key: a will get
   * a : [
   *       {text : a class unselected, count : 2}, // orig_n : 3 is thrown away after being used in the sort
   *       {text : a class, count : 4},            // orig_n : 2      "           "
   *       {text : a, count : 90}                  // orig_n : 1      "           "
   *     ]
   *  doing a reverse sort on orig_n on the values will ensure that the proper value is read first.
   *  
   * The output from the "a" reducer will then be:
   * 
   *   a  {a:90}
   *   a class {a:90}
   *   a class unselected {a:90}  
   *   
   * The output from the "a class" reducer would be
   * 
   *   a class {a class : 4}
   *   a class unselected {a class:4}
   *   
   * etc...
   * 
   * @author jmannix
   *
   */
  public class SubNGramMapper extends Mapper<Object, Text, TextAndInt, TextAndInt>
  {
    private Analyzer analyzer;
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      int maxN = context.getConfiguration().getInt("ngram.maxValue", 2);
      analyzer = new NGramAnalyzer(maxN);
      String[] ngramAndCount = value.toString().split("\t");
      
      String ngram = ngramAndCount[0];
      int inputSize = n(ngram);
      int count = Integer.parseInt(ngramAndCount[1]);
      
      TextAndInt ngramWithCount = new TextAndInt(count, ngram);
      TokenStream tokens = analyzer.tokenStream("", new StringReader(ngram)); // WTF?
      
      Token token = new Token();
      while((token = tokens.next(token)) != null)
      {
        String t = token.term();
        TextAndInt subNGram = new TextAndInt(inputSize, t);
//System.out.println("\t(output_key: \"" + subNGram.toString() + "\", output_value: " + ngramWithCount + ")");
        context.write(subNGram, ngramWithCount);
      }
    }
    
    public static final int n(String t)
    {
      int i = -1;
      int n = 1;
      while((i = t.indexOf(' ', i+1)) > 0 && n < 10) n++;
      return n;
    }
    
    public static class TextAndIntPartitioner extends Partitioner<TextAndInt, TextAndInt>
    {
      @Override
      public int getPartition(TextAndInt key, TextAndInt value, int numPartitions)
      {
        return Math.abs(key.text.toString().hashCode()) % numPartitions;
      }
    }
  }