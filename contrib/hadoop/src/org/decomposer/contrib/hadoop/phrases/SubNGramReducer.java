/**
 * 
 */
package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;


public class SubNGramReducer extends Reducer<TextAndInt, TextAndInt, Text, TextAndInt>
{
  String subKey = null;
  int value = -1;
  @Override
  public void reduce(TextAndInt key, Iterable<TextAndInt> values, Context context) throws IOException, InterruptedException
  {
    Text outputKey = new Text();
    TextAndInt outputValue = new TextAndInt();
    Iterator<TextAndInt> it = values.iterator();
    if(it.hasNext()) 
    {
      TextAndInt foo = it.next();
      String outputText = foo.text.toString();
      outputKey.set(outputText);
      if(outputText != subKey)
      {
        subKey = outputText;
        value = foo.n;
        outputValue.text.set(outputText);
        outputValue.n = foo.n; // this is the count!
      }
      else
      {
        outputValue.text.set(subKey);
        outputValue.n = value;
      }
      context.write(outputKey, outputValue);
    }
    else
    {
      throw new IllegalStateException("No values!");
    }
    while(it.hasNext())
    {
      TextAndInt foo = it.next();
      outputKey.set(foo.text.toString());
      outputValue.text.set(subKey);
      outputValue.n = value;
      context.write(outputKey, outputValue);
    }
  }
  
  public static class GroupingComparator extends WritableComparator
  {
    public GroupingComparator()
    {
      super(TextAndInt.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
      Text t1 = ((TextAndInt)w1).text;
      Text t2 = ((TextAndInt)w2).text;
      return t1.compareTo(t2);
    }
  }
  
}