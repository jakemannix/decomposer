package org.decomposer.contrib.hadoop.job;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.math.DistributedMatrix;
import org.decomposer.math.vector.MapVector;

public class MatrixViewerJob extends BaseTool
{
  
  public static void main(String[] args) throws Exception
  {
    int retVal = ToolRunner.run(new MatrixViewerJob(), args);
    System.exit(retVal);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    Properties configProps = loadJobProperties();
    
    int[] rowRange = parseRange(configProps.getProperty("matrix.rows.to.view"));
    int[] columnRange = parseRange(configProps.getProperty("matrix.columns.to.view"));
    
    DistributedMatrix corpus = new DistributedMatrix(new Path(configProps.getProperty("matrix.path")), false);
    corpus.setConf(getConf());
    
    Iterator<Entry<Integer, MapVector>> it = corpus.iterator();
    int i = 0;
    while(i < rowRange[1] && it.hasNext()) 
    {
      Entry<Integer, MapVector> entry = it.next();
      if(i >= rowRange[0])
      {
        double[] values = getValues(entry.getValue(), columnRange[0], columnRange[1]);
        System.out.println("rowNum => " + i + ", vector => " + stringifyValues(values, columnRange[0], columnRange[1]));
      }
      i++;
    }
    
    
    return 0;
  }

  private int[] parseRange(String range)
  {
    String[] s = range.split(",");
    return new int[] { Integer.parseInt(s[0]), Integer.parseInt(s[1]) };
  }
  
  private double[] getValues(MapVector vector, int startCol, int endCol)
  {
    double[] values = new double[endCol - startCol];
    for(int i=0; i<values.length; i++)
    {
      values[i] = vector.get(i+startCol);
    }
    return values;
  }
  
  private static NumberFormat format = new DecimalFormat("00.0000");
  
  private String stringifyValues(double[] values, int start, int end)
  {
    String s = "{ ";
    for(int i=0; i<values.length-1; i++)
    {
      if(values[i] != 0)
        s += "" + (i+start) + " => " + format.format(values[i]) + ", ";
    }
    if(values.length > 0)
    {
      if(values[values.length-1] != 0)
        s += "" + (end-1) + " => " + format.format(values[values.length-1]);
    }
    return s + " }";
  }
  
}
