package org.decomposer.contrib.lucene;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.decomposer.nlp.extraction.FeatureDictionary;
import org.decomposer.nlp.extraction.Idf;
import org.decomposer.nlp.extraction.FeatureDictionary.Feature;

import junit.framework.TestCase;

public class LuceneIndexDictionaryBuilderTest extends TestCase
{

  public LuceneIndexDictionaryBuilderTest(String name)
  {
    super(name);
  }

  @Override
  protected void setUp() throws Exception
  {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  public void testLuceneIndexDictionaryBuilder() throws Exception
  {
    Directory directory = buildTestDirectory();

    FeatureDictionary dictionary = LuceneIndexDictionaryBuilder.buildDictionary(directory,
                                                                                new Term("contents", ""),
                                                                                new Idf()
                                                                                {

                                                                                  public double idf(int numDocs,
                                                                                                    int count)
                                                                                  {
                                                                                    return 2.0f;
                                                                                  }
                                                                                },
                                                                                0f,
                                                                                100);
    Feature feature = dictionary.getFeature("quick");
    assertEquals(100, (int) feature.count);
    feature = dictionary.getFeature("string1");
    assertEquals(10, (int) feature.count);
    assertNull(dictionary.getFeature("otherstring99"));
    feature = dictionary.getFeature(dictionary.getNumFeatures() - 1);
    assertEquals(feature.name, "otherstring83");
  }

  private Directory buildTestDirectory() throws CorruptIndexException, LockObtainFailedException, IOException
  {
    Directory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
    for (int i = 0; i < 100; i++)
    {
      Document d = new Document();
      Field f = new Field("contents",
                          new StringReader(
                          "The quick brown fox jumped over the lazy dog: string" + (i % 10) + " otherString" + i),
                          TermVector.YES);
      d.add(f);
      writer.addDocument(d);
    }
    writer.close();
    return directory;
  }
}
