package org.decomposer.math.vector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;

public class DiskBufferedDoubleMatrix extends ImmutableStreamBufferedDoubleMatrix
{
  private static final long serialVersionUID = 1L;
  private static final Logger log = Logger.getLogger(DiskBufferedDoubleMatrix.class.getName());
  protected final File _dir;
  protected int _pageSize;
  protected boolean _isRowMatrix;
  
  protected final List<Integer> _startList = new ArrayList<Integer>();
  protected final Map<Integer, File> _files = new HashMap<Integer, File>();
  protected final int _size;
  
  public static final String ROW_PREFIX = "row";
  public static final String COL_PREFIX = "col";
  public static final String _ = "_";
  public static final String SUFFIX = ".ser";
  
  // row_ABC_to_XYZ.ser
  // col_ABC_to_XYZ.ser
  
  
  public DiskBufferedDoubleMatrix(File diskDir, int pageSize, boolean isRowMatrix)
  {
    _dir = diskDir;
    _pageSize = pageSize;
    _isRowMatrix = isRowMatrix;
    final String filePrefix = _isRowMatrix ? ROW_PREFIX : COL_PREFIX;
    if(!_dir.isDirectory()) throw new IllegalArgumentException(_dir + " is not a filesystem directory");
    int maxSize = 0;
    for(File file : _dir.listFiles(new FilenameFilter() 
    {
      public boolean accept(File dir, String name) { return name.startsWith(filePrefix); } 
    }))
    {
      if(file.canRead())
      {
        String[] parts = file.getName().split(_);
        int start = Integer.parseInt(parts[1]);
        _startList.add(start);
        _files.put(start, file);
        int end = Integer.parseInt(parts[2].replaceAll(SUFFIX, ""));
        if(end > maxSize) maxSize = end;
      }
    }
    _size = maxSize;
    Collections.sort(_startList);
  }
  
  public DiskBufferedDoubleMatrix(File diskDir, int pageSize)
  {
    this(diskDir, pageSize, true);
  }
  
  public static DoubleMatrix loadFullMatrix(File diskDir, VectorFactory factory)
  {
    DiskBufferedDoubleMatrix m = new DiskBufferedDoubleMatrix(diskDir, Integer.MAX_VALUE);
    DoubleMatrix memMatrix = new HashMapDoubleMatrix(factory);
    for(Entry<Integer, MapVector> entry : m) memMatrix.set(entry.getKey(), entry.getValue());
    return memMatrix;
  }
  
  public static void persistVector(File dir, MapVector vector, int rowNum) throws FileNotFoundException, IOException
  {
    HashMapDoubleMatrix matrix = new HashMapDoubleMatrix(new HashMapVectorFactory());
    matrix.set(rowNum, vector);
    persistChunk(dir, matrix, true);
  }
  
  public static void persistChunk(File dir, DoubleMatrix chunk, boolean isRow) throws FileNotFoundException, IOException
  {
    if(!dir.isDirectory()) throw new IllegalArgumentException(dir + " is not a filesystem directory");
    if(!dir.canWrite()) throw new IllegalArgumentException(dir + " is not writeable");
    int min = Integer.MAX_VALUE;
    int max = 0;
    for(Entry<Integer, MapVector> row : chunk)
    {
      if(row.getKey() > max) max = row.getKey();
      if(row.getKey() < min) min = row.getKey();
      if(row.getValue() instanceof HashMapVector) row.setValue(new ImmutableSparseMapVector(row.getValue()));
    }
    String prefix = isRow ? ROW_PREFIX : COL_PREFIX;
    String fileName = prefix + _ + min + _ + max + SUFFIX;
    persist(dir, fileName, chunk);
  }

  private static void persist(File dir, String fileName, DoubleMatrix chunk) throws FileNotFoundException, IOException
  {
    new ObjectOutputStream(new FileOutputStream(dir.getPath() + File.separator + fileName)).writeObject(chunk);
  }
  
  /**
   * Extraordinarily expensive, do not use for random access in inner loops!
   * @param index
   * @return the MapVector at the given index.
   */
  @Override
  public MapVector get(int index)
  {
    Iterator<Entry<Integer, MapVector>> it = newBuffer(index, 1);
    while(it.hasNext())
    {
      Entry<Integer, MapVector> e = it.next();
      if(e.getKey() < index)
      {
        continue;
      }
      else
      {
        return (e.getKey() == index) ? e.getValue() : null;
      }
    }
    return null;
  }

  @Override
  public int getPageSize()
  {
    return _pageSize;
  }

  // 0, 100, 200, 300 (size = 4)
  // 50: ipt = 1 => idx = 0
  // 250: ipt = 3 => idx = 2
  // 350: ipt = 4 => idx = 3
  
  @Override
  protected Iterator<Entry<Integer, MapVector>> newBuffer(int offset, int count)
  {
    int index = Collections.binarySearch(_startList, offset);
    if(index < 0)
    {
      index = -index - 2;
      if(index < 0) index = 0; // insertionPt was 0
    }
    List<Iterator<Entry<Integer, MapVector>>> mapList = new ArrayList<Iterator<Entry<Integer, MapVector>>>();
    int startKey = _startList.get(index);
    int currentMax = 0;
    while(startKey < offset + count || currentMax < offset + count)
    {
      File vectorFile = _files.get(_startList.get(index));
      DoubleMatrix vectors = null;
      try
      {
        vectors = load(vectorFile);
      }
      catch (IOException e)
      {
        log.warning(e.getMessage());
      }
      catch (ClassNotFoundException e)
      {
        log.warning(e.getMessage());
      }
      if(vectors != null)
      {
        for(Entry<Integer, MapVector> entry : vectors)
          currentMax = Math.max(entry.getKey(), currentMax);
        mapList.add(vectors.iterator());
      }
      index++;
      if(index < _startList.size())
        startKey = _startList.get(index);
      else
        break;
    }
    List<Entry<Integer, MapVector>> mergedList = merge(mapList);
    sort(mergedList);
    int start = 0;
    while(start < mergedList.size() && mergedList.get(start).getKey() < offset)
      start++;
    if(start >= mergedList.size()) return null;
    return mergedList.subList(start, mergedList.size()).iterator();
  }

  private void sort(List<Entry<Integer, MapVector>> mergedList)
  {
    Collections.sort(mergedList, new Comparator<Entry<Integer, MapVector>>()
                     {
                        public int compare(Entry<Integer, MapVector> o1,
                                           Entry<Integer, MapVector> o2)
                        {
                          return o1.getKey() - o2.getKey();
                        }
                     });
  }

  private <T> List<T> merge(List<Iterator<T>> list)
  {
    List<T> entries = new ArrayList<T>();
    for(Iterator<T> it : list)
    {
      while(it.hasNext())
        entries.add(it.next());
    }
    return entries;
  }

  private DoubleMatrix load(File vectorFile) throws IOException, ClassNotFoundException
  {
    ObjectInputStream ois = null;
    try
    {
      ois = new ObjectInputStream(new FileInputStream(vectorFile));
      return (DoubleMatrix) ois.readObject();
    }
    finally
    {
      if(ois != null)
      {
        ois.close();
      }
    }
  }

  @Override
  public int numRows()
  {
    return _size;
  }

  public MapVector timesSquared(MapVector vector)
  {
    MapVector w = times(vector);
    MapVector w2 = _vectorFactory.zeroVector();
    for(Entry<Integer, MapVector> entry : this)
    {
      MapVector d_i = entry.getValue();
      int i = entry.getKey();
      for(IntDoublePair pair : d_i)
      {
        int alpha = pair.getInt();
        double d_i_alpha = pair.getDouble();
        w2.add(alpha, w.get(i) * d_i_alpha);
      }
    }
    return w2;
  }
  
  public void delete()
  {
    delete(_dir);
  }
  
  public static void delete(File file)
  {
    if(file.isDirectory())
    {
      for(File f : file.listFiles())
      {
        delete(f);
      }
    }
    file.delete();
  }

}
