package org.decomposer.contrib.hadoop.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.math.DistributedMatrix;
import org.decomposer.math.LanczosSolver;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.MapVector;

public class LanczosJob extends BaseTool
{
  
  public static void main(String[] args) throws Exception
  {
    int ret = ToolRunner.run(new LanczosJob(), args);
    System.exit(ret);
  }
  
  @Override
  public int run(String[] args) throws Exception
  {
    Properties configProps = loadJobProperties();
    
    DistributedMatrix corpus = new DistributedMatrix(new Path((String) configProps.get("corpus.input.path")));
    corpus.setConf(getConf());
    DistributedMatrix eigenVectors = new DistributedMatrix(new Path((String) configProps.get("eigen.output.path") + "/" + getCurrentTimeStamp()));
    eigenVectors.setConf(getConf());
    List<Double> eigenValues = new ArrayList<Double>();
    
    LanczosSolver solver = new LanczosSolver()
    {
      @Override
      protected MapVector getInitialVector(DoubleMatrix c)
      {
        MapVector initialVector = ((DistributedMatrix)c).hebbianPass(null);
        initialVector.scale(1/initialVector.norm());
        return initialVector;
      }
    };
    
    solver.solve(corpus, Integer.parseInt((String)configProps.get("desired.rank")), eigenVectors, eigenValues);
    eigenVectors.distribute();
    
    return 0;
  }

}
