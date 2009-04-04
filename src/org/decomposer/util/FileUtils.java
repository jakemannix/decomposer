package org.decomposer.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Hacky set of file I/O and serialization helper methods.
 * @author jmannix
 */
public class FileUtils
{
  public static File createTmpDir(String subDir, boolean deleteFirst)
  {
    File sysTemp = new File(System.getProperty("java.io.tmpdir"));
    File tmp = new File(sysTemp, subDir);
    if(deleteFirst && writeable(tmp)) delete(tmp);
    tmp.mkdir();
    return tmp;
  }
  
  public static void delete(File file)
  {
    if(writeable(file))
    {
      if(file.isDirectory())
      {
        for(File f : file.listFiles()) delete(f);
      }
      file.delete();
    }
  }
  
  public static boolean writeable(File file)
  {
    return (file != null && file.exists() && file.canWrite());
  }
  
  public static void serialize(Serializable s, File dir, String fileName) throws FileNotFoundException, IOException
  {
    new ObjectOutputStream(new FileOutputStream(dir.getPath() + File.separator + fileName)).writeObject(s);
  }
  
  public static <T extends Serializable> T deserialize(Class<T> clazz, File file) throws FileNotFoundException, IOException, ClassNotFoundException
  {
    ObjectInputStream ois = null;
    try
    {
      ois = new ObjectInputStream(new FileInputStream(file));
      return (T) ois.readObject();
    }
    finally
    {
      if(ois != null)
      {
        ois.close();
      }
    }
  }
}
