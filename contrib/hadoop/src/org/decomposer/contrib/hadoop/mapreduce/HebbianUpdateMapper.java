package org.decomposer.contrib.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;

public class HebbianUpdateMapper extends MatrixMultiplyMapper
{

  /**
   * input value is the row of the matrix to multiply by.  No output yet, just accumulate!
   */
  @Override
  public void map(Object key, MapVectorWritableComparable value, Context context) throws IOException, InterruptedException
  {
    if(output.norm() == 0)
    {
      if(inputVector.norm() > 0)
      {
        output.plus(inputVector);
        output.plus(value, output.dot(value));
      }
      else
      {
        output.plus(value);
      }
    }
    else
    {
      output.plus(value, value.dot(output));
    }
  }
  
  /**
   * when we're done with all of the rows accessible to this Mapper, we finally write out the accumulatedOutput
   */
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException 
  {
    context.write(new LongWritable(-1), output);
  }
}
