package org.decomposer.contrib.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.decomposer.contrib.hadoop.math.IntDoublePairWritableComparable;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.hashmap.HashMapVector;

public class MatrixTransposeReducer extends Reducer<LongWritable, IntDoublePairWritableComparable, LongWritable, MapVectorWritableComparable>
{
  private MapVectorWritableComparable column = new MapVectorWritableComparable(new HashMapVector());
  
  @Override
  public void reduce(LongWritable columnNum, Iterable<IntDoublePairWritableComparable> columnValues, Context context) throws IOException, InterruptedException
  {
    for(IntDoublePairWritableComparable pair : columnValues)
      column.set(pair.getInt(), pair.getDouble());
    column.setRow(columnNum.get());
    context.write(columnNum, column);
  }
  
}
