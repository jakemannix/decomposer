package org.decomposer.contrib.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;

public class BasisOrthogonalizeReducer extends Reducer<LongWritable, MapVectorWritableComparable, LongWritable, MapVectorWritableComparable>
{
  protected MapVectorWritableComparable inputVector;
  protected MapVectorWritableComparable output = new MapVectorWritableComparable();
  protected static final LongWritable negativeOne = new LongWritable(-1);

  @Override
  protected void setup(Context context) throws IOException
  {
    inputVector = new MapVectorWritableComparable(new Path(context.getConfiguration().get("inputVector")));
    inputVector.localize();
  }
  
  public void reduce(LongWritable key, Iterable<MapVectorWritableComparable> values, Context context)
  {
    for(MapVectorWritableComparable v : values) output.plus(v);
  }
  
  protected void cleanup(Context context) throws IOException, InterruptedException
  {
    context.write(negativeOne, output);
  }
  
}
