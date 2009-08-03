package org.decomposer.contrib.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.array.DenseMapVector;

public class MatrixMultiplyReducer extends Reducer<LongWritable, MapVectorWritableComparable, LongWritable, MapVectorWritableComparable>
{
  MapVectorWritableComparable output = new MapVectorWritableComparable(new DenseMapVector(), -1L);
  
  @Override
  public void reduce(LongWritable key, Iterable<MapVectorWritableComparable> values, Context context) throws IOException, InterruptedException
  {
    for(MapVectorWritableComparable value : values) output.plus(value);
    context.write(key, output);
  }
  
}
