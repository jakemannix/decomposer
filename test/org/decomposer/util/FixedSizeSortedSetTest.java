package org.decomposer.util;

import java.util.Comparator;
import java.util.Set;

import junit.framework.TestCase;

public class FixedSizeSortedSetTest extends TestCase
{
  public FixedSizeSortedSetTest(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    super.setUp();
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }
  
  public void testFixedSizeSortedSet() throws Exception
  {
    Set<Integer> ints = new FixedSizeSortedSet<Integer>(new Comparator<Integer>()
        {
          public int compare(Integer o1, Integer o2)
          {
            return o1 - o2;
          }
        }, 10);
    
    for(Integer i=0; i<25; i++) ints.add(i.toString().hashCode());
    assertEquals(10, ints.size());
    int earlier = ints.iterator().next();
    for(int in : ints)
    {
      assertTrue(in >= earlier);
      System.out.println(in);
      earlier = in;
    }
  }

}
