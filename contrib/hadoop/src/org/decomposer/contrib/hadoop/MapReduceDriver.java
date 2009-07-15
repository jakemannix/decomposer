package org.decomposer.contrib.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.util.ProgramDriver;

public class MapReduceDriver
{  
  private static final Logger log = Logger.getLogger(MapReduceDriver.class.getName());
  
  public static void main(String[] args) throws Exception
  {
    int exitCode = -1;
    try
    {
      ProgramDriver programDriver = new ProgramDriver();
      Properties mainClasses = new Properties();
      boolean shiftArgs = false;
      String propsFileName = (args[0].endsWith(".props") || args[0].endsWith(".properties")) && (shiftArgs = true) 
                           ? args[0] 
                           : "contrib/hadoop/resources/map.reduce.classes.props";
      
      mainClasses.load(new FileInputStream(propsFileName));
      for(Object key :  mainClasses.keySet())
      {
        String keyString = (String) key; 
        addClass(programDriver, keyString, (String)mainClasses.get(keyString));
      }
      String[] moreArgs;
      if(shiftArgs)
      {
        moreArgs = new String[args.length - 1];
        System.arraycopy(args, 1, moreArgs, 0, moreArgs.length);
      }
      else
      {
        moreArgs = args;
      }
      programDriver.driver(moreArgs);
      exitCode = 0;
    }
    catch (Throwable e)
    {
      log.severe("MapReduceDriver failed with args: " + Arrays.toString(args) + "\n" + e.getMessage());
      exitCode = -1;
    }
        
    System.exit(exitCode);
  }

  private static void addClass(ProgramDriver driver, String classString, String descString)
  {
    try
    {
      Class<?> clazz = Class.forName(classString);
      driver.addClass(clazz.getSimpleName(), clazz, descString);
    }
    catch (Throwable e)
    {
      log.warning("Unable to add class: " + classString + "\n" + e.getMessage());
    }
  }
}
