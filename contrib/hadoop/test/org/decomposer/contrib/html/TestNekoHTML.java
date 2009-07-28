package org.decomposer.contrib.html;

import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.decomposer.contrib.hadoop.phrases.HTMLNGramAnalyzer;
import org.decomposer.contrib.hadoop.phrases.NGramAnalyzer;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.html.HTMLDocument;
import org.xml.sax.InputSource;

import junit.framework.TestCase;

public class TestNekoHTML extends TestCase
{

  private static String htmlFragment = 
  "<p><b>Aztec</b> is a term used to refer to certain ethnic groups of central <a href=\"../../wp/m/Mexico.htm\" title=\"Mexico\">M" +
  "exico</a>, particularly those groups who spoke the <a class=\"mw-redirect\" href=\"../../wp/n/Nahuatl.htm\" title=\"Nahuatl language\">N" +
  "ahuatl language</a> and who achieved political and military dominance over large parts of <!--del_lnk--> Mesoamerica in the 14th, " +
  "15th and 16th centuries, a period referred to as the Late post-Classic period in <!--del_lnk--> Mesoamerican chronology.<p>Often t" +
  "he term &quot;Aztec&quot; refers exclusively to the people of <!--del_lnk--> Tenochtitlan, situated on an island in <!--del_lnk-->" +
  " Lake Texcoco, who called themselves <i>Mexica Tenochca</i> or Colhua-Mexica.<p>Sometimes it also includes the inhabitants of Teno" +
  "chtitlan&#39;s two principal allied city-states, the <!--del_lnk--> Acolhuas of <!--del_lnk--> Texcoco and the <!--del_lnk--> Tepa" +
  "necs of <!--del_lnk--> Tlacopan, who together with the Mexica formed the <!--del_lnk--> Aztec Triple Alliance which has also becom" +
  "e known as the &quot;<b>Aztec Empire</b>&quot;. In other contexts it may refer to all the various <!--del_lnk--> city states and t" +
  "heir peoples, who shared large parts of their ethnic history as well as many important cultural traits with the Mexica, Acolhua an" +
  "d Tepanecs, and who like them, also spoke the Nahuatl language. In this meaning it is possible to talk about an <b>Aztec civilizat" +
  "ion</b> including all the particular cultural patterns common for the Nahuatl speaking peoples of the late postclassic period in M" +
  "esoamerica.<p>From the 12th century the <!--del_lnk--> Valley of Mexico was the nucleus of Aztec civilization: here the capital of" +
  " the Aztec Triple Alliance, the city of <!--del_lnk--> Tenochtitlan, was built upon raised islets in <!--del_lnk--> Lake Texcoco. " +
  "The Triple Alliance formed its tributary empire expanding its political hegemony far beyond the Valley of Mexico, conquering other" +
  " city states throughout Mesoamerica.<p>At its pinnacle Aztec culture had rich and complex <!--del_lnk--> mythological and <!--del_" +
  "lnk--> religious traditions, as well as reaching remarkable architectural and artistic accomplishments. A particularly striking el" +
  "ement of Aztec culture to many was the practice of <!--del_lnk--> human sacrifice.<p>In 1521, in what is probably the most widely " +
  "known episode in the <!--del_lnk--> Spanish colonization of the Americas, <a href=\"../../wp/h/Hern%25C3%25A1n_Cort%25C3%25A9s.htm\" "+
  " title=\"Hern&aacute;n Cort&eacute;s\">Hern&aacute;n Cort&eacute;s</a>, along with a large number of Nahuatl speaking indigenous all" +
  "ies, conquered Tenochtitlan and defeated the Aztec Triple Alliance under the leadership of <!--del_lnk--> Hueyi Tlatoani <!--del_l" +
  "nk--> Moctezuma II; In the series of events often referred to as &quot;<!--del_lnk--> The Fall of the Aztec Empire&quot;. Subseque" +
  "ntly the Spanish founded the new settlement of <a href=\"../../wp/m/Mexico_City.htm\" title=\"Mexico City\">Mexico City</a> on the sit" +
  "e of the ruined Aztec capital.<p>Aztec culture and history is primarily known:<ul>";

  private static String htmlWithCDATA = 
    "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">" + 
    "<html dir=\"ltr\" lang=\"en\" xml:lang=\"en\" xmlns=\"http://www.w3.org/1999/xhtml\"> " + 
    "<head>  <meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\" />  " + 
    "<meta content=\"Auckland,Articles with unsourced statements since May 2008,Territorial Authorities of New Zealand,New Zealand" + 
    "topics,Commonwealth Games Host Cities,1930 Commonwealth Games,1934 Commonwealth Games,1938 Commonwealth Games,1939,1950 British" + 
    " Empire Games,1950 Commonwealth Games\" name=\"keywords\" />  <link href=\"../../favicon.ico\" rel=\"shortcut icon\" />  " +
    " <link href=\"../../apple-touch-icon.png\" rel=\"apple-touch-icon\" />  <link href=\"../../wp/w/Wikipedia%253AText_of_the_GNU_Free_Documentation_License.htm\" rel=\"copyright\" />" + 
    " <title>Auckland</title>  <style media=\"screen, projection\" type=\"text/css\">/*<![CDATA[*/                        " + 
    "@import \"../../css/wp-commonShared.css\";                        @import \"../../css/wp-monobook-main.css\";               /*]]>*/</style>" + 
    "  <link href=\"../../css/wp-commonPrint.css\" media=\"print\" rel=\"stylesheet\" type=\"text/css\" />  <!--[if lt IE 5.5000]><style type=\"text/css\">@import " + 
    " \"../../css/IE50Fixes.css\";</style><![endif]-->  <!--[if IE 5.5000]><style type=\"text/css\">@import \"../../css/IE55Fixes.css\";</style><![endif]--> " + 
    "  <!--[if IE 6]><style type=\"text/css\">@import \"../../css/IE60Fixes.css\";</style><![endif]-->  <!--[if IE 7]><style type=\"text/css\">@import \"../../css/IE70Fixes.css\";</style><![endif]-->" + 
    "  <!--[if lt IE 7]><script type=\"text/javascript\" src=\"../../js/IEFixes.js\"></script>             " + 
    " <meta http-equiv=\"imagetoolbar\" content=\"no\" /><![endif]-->  <script src=\"../../js/wikibits.js\" type=\"text/javascript\"><!-- wikibits js --></script> " + 
    "  <!-- Head Scripts -->  <script src=\"../../js/wp.js\" type=\"text/javascript\"><!-- site js --></script>  <style type=\"text/css\">" + 
    "/*<![CDATA[*/@import \"../../css/wp-common.css\";@import \"../../css/wp-monobook.css\";@import \"../../css/wp.css\";/*]]>*/</style> " + 
    " </head> <body class=\"mediawiki ns-0 ltr page-Auckland\">  <div id=\"globalWrapper\">   <div id=\"column-content\">   " + 
    " <div id=\"content\"><a id=\"top\" name=\"top\"></a><h1 class=\"firstHeading\">Auckland</h1>     <div id=\"bodyContent\">    " + 
    " <h3 id=\"siteSub\"><a href=\"../../index.htm\">2008/9 Schools Wikipedia Selection</a>. Related subjects: " + 
    " <a href=\"../index/subject.Geography.Geography_of_Oceania_Australasia.htm\">Geography of Oceania (Australasia)</a></h3>  " +  
    "     <!-- start content -->      <table class=\"infobox geography vcard\" style=\"width:23em; text-align:left;\">"; 
   
  String foo = "<title>Auckland</title>  <style media=\"screen, projection\" type=\"text/css\">/*         */</style>  <link href=\"../../css/wp-commonPrint.css\" media=\"print\" rel=\"stylesheet\" type=\"text/css\" /> ";
  
  String postCommentRemoved = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\"><html dir=\"ltr\" lang=\"en\" xml:lang=\"en\" xmlns=\"http://www.w3.org/1999/xhtml\"> <head>  <meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\" />  <meta content=\"Auckland,Articles with unsourced statements since May 2008,Territorial Authorities of New Zealand,New Zealandtopics,Commonwealth Games Host Cities,1930 Commonwealth Games,1934 Commonwealth Games,1938 Commonwealth Games,1939,1950 British Empire Games,1950 Commonwealth Games\" name=\"keywords\" />  <link href=\"../../favicon.ico\" rel=\"shortcut icon\" />   <link href=\"../../apple-touch-icon.png\" rel=\"apple-touch-icon\" />  <link href=\"../../wp/w/Wikipedia%253AText_of_the_GNU_Free_Documentation_License.htm\" rel=\"copyright\" /> <title>Auckland</title>  <style media=\"screen, projection\" type=\"text/css\">/*    */</style>  <link href=\"../../css/wp-commonPrint.css\" media=\"print\" rel=\"stylesheet\" type=\"text/css\" /> <script src=\"../../js/wikibits.js\" type=\"text/javascript\">  </script>  <script src=\"../../js/wp.js\" type=\"text/javascript\"> </script>  <style type=\"text/css\">/*      */</style>  </head> <body class=\"mediawiki ns-0 ltr page-Auckland\">  <div id=\"globalWrapper\">   <div id=\"column-content\">    <div id=\"content\"><a id=\"top\" name=\"top\"></a><h1 class=\"firstHeading\">Auckland</h1>     <div id=\"bodyContent\">     <h3 id=\"siteSub\"><a href=\"../../index.htm\">2008/9 Schools Wikipedia Selection</a>. Related subjects:  <a href=\"../index/subject.Geography.Geography_of_Oceania_Australasia.htm\">Geography of Oceania (Australasia)</a></h3> <table class=\"infobox geography vcard\" style=\"width:23em; text-align:left;\">";
  
  public TestNekoHTML(String name)
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
  
  public void testFoo() throws Exception
  {
    DOMFragmentParser parser = new DOMFragmentParser();
    HTMLDocument document = new HTMLDocumentImpl();
    DocumentFragment fragment = document.createDocumentFragment();
    parser.parse(new InputSource(new StringReader(postCommentRemoved)), fragment);
    String text = fragment.getTextContent();
    System.out.println(text);
  }
  
  public void testNekoHtml() throws Exception
  {
    DOMFragmentParser parser = new DOMFragmentParser();
    HTMLDocument document = new HTMLDocumentImpl();
    DocumentFragment fragment = document.createDocumentFragment();
    parser.parse(new InputSource(new StringReader(htmlWithCDATA.replace(">", "> "))), fragment);
    String text = fragment.getTextContent();
    System.out.println(text);
    
    NGramAnalyzer analyzer = new NGramAnalyzer(2);
    TokenStream stream = analyzer.tokenStream("", new StringReader(text));
    Token token = new Token();
    while((token = stream.next(token)) != null)
    {
      System.out.println(token.term());
    }
  }
  
  public void testHTMLNGramAnalyzer() throws Exception
  {
    TokenStream stream = new HTMLNGramAnalyzer(2).tokenStream(new Text(htmlWithCDATA));
    Token token = new Token();
    StringBuffer buf = new StringBuffer();
    while((token = stream.next(token)) != null)
    {
      buf.append(token.term()).append(' ');
    }
    System.out.println(buf.toString());
  }
  
  public void testHTMLReader() throws Exception
  {
    TokenStream stream = new WhitespaceTokenizer(new HTMLNGramAnalyzer.HTMLTextReader(new Text(htmlWithCDATA)));

    Token token = new Token();
    StringBuffer buf = new StringBuffer();
    while((token = stream.next(token)) != null)
    {
      buf.append(token.term()).append(" : ");
    }
    System.out.println(buf.toString());
  }

}
