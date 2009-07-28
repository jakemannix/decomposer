package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;


public class TermCountingMapper extends Mapper<Object, Text, Text, IntWritable>
{
  private final static IntWritable one  = new IntWritable(1);
  private Text                     word = new Text();
  
  private int maxNGram;
  private boolean inputIsHtml;
  
  public void setup(Context context) 
  {
    maxNGram = context.getConfiguration().getInt("ngram.maxValue", 2);
    inputIsHtml = context.getConfiguration().getBoolean("input.isHtml", false);
  }

private static final Set<String> htmlStrings = new HashSet<String>();
static
{
  htmlStrings.add("del");
  htmlStrings.add("lnk");
  htmlStrings.add("href");
  htmlStrings.add("wp");
  htmlStrings.add("img");
  htmlStrings.add("alt");
  htmlStrings.add("div");
  htmlStrings.add("align");
  htmlStrings.add("png");
  htmlStrings.add("thumbcaption");
  htmlStrings.add("td align");
  htmlStrings.add("span class");
  htmlStrings.add("href");
}

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException
  {
    TokenStream stream = inputIsHtml 
                       ? new HTMLNGramAnalyzer(maxNGram).tokenStream(value)
                       : new NGramAnalyzer(maxNGram).tokenStream("", new TextReader(value));
    Token token = new Token();
    while ((token = stream.next(token)) != null)
    {
      if(htmlStrings.contains(token.term())) continue;
      word.set(token.term());
      context.write(word, one);
    }
  }
}
