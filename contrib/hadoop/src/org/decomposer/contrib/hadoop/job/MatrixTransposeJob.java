package org.decomposer.contrib.hadoop.job;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.math.DistributedMatrix;

public class MatrixTransposeJob extends BaseTool
{
  
  public static void main(String[] args) throws Exception
  {
    System.exit(ToolRunner.run(new MatrixTransposeJob(), args));
  }
  
  @Override
  public int run(String[] args) throws Exception
  {
    return new DistributedMatrix(new Path((String) loadJobProperties().get("corpus.input.path")), 
                                 getConf()).transpose() != null ? 0 : -1;
  }

}
