package org.decomposer.contrib.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;

public class BasisOrthogonalizeMapper extends Mapper<LongWritable, MapVectorWritableComparable, LongWritable, MapVectorWritableComparable>
{
  protected MapVectorWritableComparable inputVector;
  protected MapVectorWritableComparable outputVector = new MapVectorWritableComparable();
  protected static final LongWritable negativeOne = new LongWritable(-1);
  @Override
  protected void setup(Context context) throws IOException
  {
    inputVector = new MapVectorWritableComparable(new Path(context.getConfiguration().get("inputVector")));
    inputVector.localize();
  }
  
  @Override
  public void map(LongWritable key, MapVectorWritableComparable basisVector, Context context) throws IOException, InterruptedException
  {
    outputVector.plus(basisVector, -basisVector.dot(inputVector));
    context.write(negativeOne, outputVector);
  }

}
