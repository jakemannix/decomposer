package org.decomposer.contrib.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.array.DenseMapVector;

public class MatrixMultiplyMapper extends Mapper<Object, MapVectorWritableComparable, LongWritable, MapVectorWritableComparable>
{
  protected MapVectorWritableComparable inputVector;
  protected MapVectorWritableComparable output;
  
  /**
   * Loads up, from the DistributedCache, the previous vector (which you are multiplying by)
   * @param context
   * @throws IOException
   */
  protected void loadVectors(Configuration config) throws IOException
  {
    Path inputVectorPath = new Path(config.get("inputVector"));
    inputVector = new MapVectorWritableComparable(inputVectorPath);
    inputVector.setConf(config);
    inputVector.localize();
  }
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException
  {
    loadVectors(context.getConfiguration());
    output = new MapVectorWritableComparable(new DenseMapVector(), -1L);
    output.setConf(context.getConfiguration());
  }


  /**
   * input value is the row of the matrix to multiply by.  No output yet, just accumulate!
   */
  @Override
  public void map(Object key, MapVectorWritableComparable value, Context context) throws IOException, InterruptedException
  {
    output.plus(value, value.dot(inputVector));
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
