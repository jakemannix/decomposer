/**
 * 
 */
package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public final class NonComputingMLReducer extends Reducer<Text, Text, Text, Text>
{
  @Override
  public void reduce(Text ngram, Iterable<Text> jsons, Context context) throws IOException, InterruptedException
  {
    Text output = new Text();
    Text text = new Text(ngram.toString());
    StringBuffer valueBuffer = new StringBuffer();
    for(Text json : jsons)
    {
      valueBuffer.append(json.toString()).append(", ");
    }
    output.set(valueBuffer.toString());
    context.write(text, output);
  }
}