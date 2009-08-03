package org.decomposer.contrib.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.decomposer.contrib.hadoop.math.IntDoublePairWritableComparable;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.IntDoublePair;

public class MatrixTransposeMapper extends Mapper<LongWritable, MapVectorWritableComparable, LongWritable, IntDoublePairWritableComparable>
{
  LongWritable column = new LongWritable();
  IntDoublePairWritableComparable columnValue; 
  @Override
  public void map(LongWritable rowNum, MapVectorWritableComparable row, Context context) throws IOException, InterruptedException
  {
    columnValue = new IntDoublePairWritableComparable((int) rowNum.get(), 0);
    for(IntDoublePair pair : row)
    {
      column.set(pair.getInt());
      columnValue.setDouble(pair.getDouble());
      context.write(column, columnValue);
    }
  }
}
