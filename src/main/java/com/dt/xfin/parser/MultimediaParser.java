package com.dt.xfin.parser;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.dt.xfin.solr.dataImporter;


public class MultimediaParser {
	
	 static public String XML_TYPE="xml";
	  static public String HTML_TYPE="html";
	  static public String TEXT="text";
	  Metadata metadata;
	  
	  public Metadata getMetadata() {
		return metadata;
	}

	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}

	private static String currentOutputFolder;
	
	/**
	 * a holder for current contents
	 */
	private static List<Object> currentContents=new ArrayList<Object>();
	

	public static List<Object> getCurrentContents() {
		return currentContents;
	}

	/**
	 * 
	 * @param currentContents current content of data being processed. 
	 * used by parser to put the data in, when extract-individual is enable
	 */
	public static void setCurrentContents(List<Object> currentContents) {
		MultimediaParser.currentContents = currentContents;
	}

	//web_url_prefix for embedded images/documents
	  private static String  web_url_prefix="";
	  
	  private static String  WEB_CONTENT_ROOT_FOLDER="";
	  
	  private static String extractDir;
	  
	  private static String currentFile="";
	  
	 public static String getCurrentFilePath() {
		return currentFile;
	}

	/**
	  * 
	  * @return web_url_prefix for embedded images extracted from document
	  */
	public  static String getWeb_url_prefix() {
		return MultimediaParser.web_url_prefix;
	}

	public static String getCurrentOutputFolder() {
		return currentOutputFolder;
	}
	
	/**
	 * 
	 * @param filename file name
	 * @return encode file name with web_url_prefix and current output folder
	 */
	public static String getUrlOutputFileName(String filename){
		//<img src="http://localhost:7979/media/TITAN/PROCESSED_FILES/Hezbollah%2520%28OSC%2520guide%2520to%2520leaderhsip%29//opt/minimal/solrFinops/webapps/media/TITAN/PROCESSED_FILES/Hezbollah%2520%28OSC%2520guide%2520to%2520leaderhsip%29/9_Im0.Image" width="250"/>UNCLASSIFIED//FOUO 
		//		</p>
		
		String tmp=filename;
		try {
		//construct the web url for image source
		if(  MultimediaParser.getWeb_url_prefix().length()>0)
		{
			 tmp=MultimediaParser.getWeb_url_prefix()+File.separatorChar;
			 //start url from media
			 if(filename.indexOf("media")>0)
			 {
				 tmp+=filename.substring(filename.indexOf("media"));
			 }
			 //file name includes web content root folder, go ahead remove the we content root folder from url
			 else if(filename.startsWith(dataImporter.getWEB_CONTENT_ROOT_FOLDER()))
			 {
				 tmp+=filename.substring(dataImporter.getWEB_CONTENT_ROOT_FOLDER().length());
			 }
			//go the relative path inside the web content 
			 else if(MultimediaParser.getCurrentOutputFolder().startsWith(dataImporter.getWEB_CONTENT_ROOT_FOLDER()))
			{
				tmp+=MultimediaParser.getCurrentOutputFolder().substring(dataImporter.getWEB_CONTENT_ROOT_FOLDER().length());
				tmp+="/"+filename;
			}
			
			 else
			 {
				 tmp+=filename;
			 }
			
		
		}
	
		//	tmp=dataImporter.encode(tmp);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tmp;
	}
	/*
	 *  * @param web_url_prefix for embedded images/documents
	 */
	public static void setWeb_url_prefix(String web_url_prefix) {
		if(web_url_prefix!=null)
		{
			MultimediaParser.web_url_prefix = web_url_prefix;
		}
	}


	private   Detector detector;
	private    AutoDetectParser   parser;

	/**
	 * multi media parser. use default Detector, and AutoDetectParser
	 */
	public MultimediaParser()
	{
		detector = new DefaultDetector();
		parser = new AutoDetectParser(detector);
		metadata = new Metadata();
	}
	
	public MultimediaParser(Detector detector , AutoDetectParser   parser)
	{
		this.detector=detector;
		this.parser=parser;
		metadata = new Metadata();
	}
	private static void log(String msg) {
		
		  if(  Logger.getLogger(MultimediaParser.class).isDebugEnabled())
		  Logger.getLogger(MultimediaParser.class).debug(msg);
	   
	  }
	
	/**
	 * 
	 * @param input input file/folder to extract the metadata, embedded objected from 
	 * @param output output forder to put the  html, and embedded file to.
	 * @param outputType TEXT, XML, HTML(XML format with embedded images/object and .html file in output folder)
	
	 * 
	 */
	 public   String processFile(File input, File output, String outputType){
	
	
		 
	        StringWriter xmlBuffer = new StringWriter();
		      //create a xml/html content handler
	        ContentHandler handler=null;
			try {
				if(outputType==TEXT)
				{
					handler = new BodyContentHandler(100*1024*1024);
				}
				else
				{
					handler = getXmlContentHandler(xmlBuffer);
				}
			} catch (TransformerConfigurationException e1) {
				
				e1.printStackTrace();
			}
	        
		
       
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
     //   FileEmbeddedDocumentExtractor extractor=new FileEmbeddedDocumentExtractor();
      
        InputStream stream=null;
     URL url=null;
		try {
			//get the Url representation of file path. note the file.filesperate is always "/" for window and linux in url.
			 url=input.toURI().toURL();
			  currentFile=input.getAbsolutePath();
			  
			  
			  log("processing :" +currentFile);
			  
				 if(output==null)
			 		{
					 
			
					 
					 
					 
			 		 // output=new File(url.getFile().substring(0, url.getFile().lastIndexOf(".")));	
			 		 int idx=url.getFile().lastIndexOf("/");
			 		 //int idx=input.getAbsolutePath().lastIndexOf(File.separator);
			 		  
			 		  if(idx<0)
			 		  {
			 			// idx=input.getAbsolutePath().length();
			 			 idx=url.getFile().length();
			 			//  log("can't find last Indexof "+File.separatorChar+" in file: "+url.getFile()+" . use length");
			 		  }
			 		 // int lastIndex=input.getAbsolutePath().lastIndexOf(".");
			 		  int lastIndex=url.getFile().lastIndexOf(".");
			 		  if(lastIndex<0)
			 		  {
			 			 //lastIndex=input.getAbsolutePath().length();
						lastIndex=url.getFile().length();
						// log("can't find last Indexof . in file: "+url.getFile()+" . use length");
			 		  }
					 
					String str=url.getFile().substring(0, idx);
					// String str=input.getAbsolutePath().substring(0, idx);
					 
					 str+="/"+dataImporter.PROCESSED_FILES_FOLDER+
						//	 input.getAbsolutePath().substring(idx+1, lastIndex).replaceAll(" ", "");
					 
							 url.getFile().substring(idx,
									 lastIndex);
									
					 //go ahead remove all empty space
					
					 
					 output=new File(str);
			 		  output.mkdirs();
			 		 
			 		}
				 else
				 {
					 int lastIndex=url.getFile().lastIndexOf("/");
					 
					 int dx=url.getFile().indexOf(".");
					 if(dx<0)
					 {
						 dx=url.getFile().length();
					 }
					 output=new File(output+url.getFile().substring(lastIndex, dx));
					 log("output: "+output+" last index of filesperator is "+lastIndex+" indx of . is"+dx);
					 
					
			 		  output.mkdirs();
				 }
				 currentOutputFolder=output.getPath();
				 // extractor.setExtractDir(output);
				 // context.set(EmbeddedDocumentExtractor.class, extractor);
				
			stream =
			TikaInputStream.get(url, metadata);
				parser.parse(stream, handler, metadata, context);
				
			
			 
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
		catch (IOException e) {
		
			e.printStackTrace();
		} catch (SAXException e) {
			
			e.printStackTrace();
		} catch (TikaException e) {
			
			e.printStackTrace();
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
        	  if(stream!=null)
        	  {
        		  try {
        			  stream.close();
        		  } catch (IOException e) {
			
        			  e.printStackTrace();
        		  }
        	  }
        		
        String content = xmlBuffer.toString();
        //replace empty title. some browser doesn't handle empty title. 
        content= content.replace("<title/>", "<title>LESNET FOUO File</title>");
        //preserve the original document format
        content=content.replace("<body>", "<body><pre>").replace("</body>", "</pre></body>");
        content=content.replace("<BODY>", "<BODY><pre>").replace("</BODY>", "</pre></BODY>");
       
        return content;
	}
 
    
	  private  ContentHandler getXmlContentHandler(Writer writer)
	            throws TransformerConfigurationException {
	        SAXTransformerFactory factory = (SAXTransformerFactory)
	            SAXTransformerFactory.newInstance();
	        TransformerHandler handler = factory.newTransformerHandler();
	        handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "xml");
	        handler.setResult(new StreamResult(writer));
	        return handler;
	    }
	
	 
}
