package org.decomposer.contrib.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.decomposer.math.vector.array.DenseMapVector;

public class MatrixMultiplyReducer extends Reducer<NullWritable, DenseVectorWritableComparable, NullWritable, DenseVectorWritableComparable>
{
  DenseVectorWritableComparable output = new DenseVectorWritableComparable();
  
  @Override
  public void reduce(NullWritable key, Iterable<DenseVectorWritableComparable> values, Context context) throws IOException, InterruptedException
  {
    for(DenseVectorWritableComparable value : values)
    {
      output.setVector((DenseMapVector)value.getVector().plus(output.getVector()));
    }
    context.write(NullWritable.get(), output);
  }
  
}
