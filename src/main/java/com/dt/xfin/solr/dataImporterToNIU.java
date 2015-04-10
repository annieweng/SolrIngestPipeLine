package com.dt.xfin.solr;


import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;

import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.microsoft.Cell;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.dt.xfin.data.PDFData;
import com.dt.xfin.data.PowerPointSlide;
import com.dt.xfin.parser.MultimediaParser;

import java.awt.Point;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/* Example class showing the skeleton of using Tika and
   Sql on the client to index documents from
   both structured documents and a SQL database.

   NOTE: The SQL example and the Tika example are entirely orthogonal.
   Both are included here to make a
   more interesting example, but you can omit either of them.
<field name="database_nm" type="string" indexed="true" stored="true"/>
 <field name="table_nm" type="string" indexed="true" stored="true"/>
 <field name="source_nm" type="string" indexed="true" stored="true"/>
 <field name="mediaFormat" type="string" indexed="true" stored="true"/>
 <!-- image link --> 
 <field name="image_links" type="string" stored="true" multiValued="true"/>
 <field name="links" type="string" indexed="true" stored="true" multiValued="true"/>

 <!-- Main body of document extracted by SolrCell.
        NOTE: This field is not indexed by default, since it is also copied to "text"
        using copyField below. This is to save space. Use this field for returning and
        highlighting document content. Use the "text" field to search the content. -->
 <field name="content" type="text_general" indexed="false" stored="true" multiValued="true"/>
 <!-- catchall field, containing all other searchable text fields (implemented
        via copyField further on in this schema  -->
 <field name="text" type="text_general" indexed="true" stored="false" multiValued="true"  termVectors="true" termPositions="true" termOffsets="true"/>

 <!-- catch all field for database date type data. use to facet date in general-->
 <field name="date_text" type="tdate" indexed="true" stored="false" multiValued="true"/>

  <!-- catch all field for database currency data. used to facet money in general-->
  <field name="money_text" type="tdouble" indexed="true" stored="false" multiValued="true"/>
  
  
  <!-- catchall text field that indexes tokens both normally and in reverse for efficient
        leading wildcard queries. -->
  <field name="text_rev" type="text_general_rev" indexed="true" stored="false" multiValued="true" />

 */
public class dataImporterToNIU {
	//private StreamingUpdateSolrServer _server;
	private SolrServer _server = null;
	private Connection con = null;
	private PreparedStatement niuDocStmt=null;

	private long _start = System.currentTimeMillis();
	private static String UNIQ_ID = "XFIN_ID";
	private static  File inputDir;
	private  static File extractDir;
	public static String collectionURL=null;

	private static String outputType=MultimediaParser.TEXT;
	private int _totalTika = 0;
	private int _totalSql = 0;
	public static String databasehost="dbstage";
	private static String zookeeper_url=null;
	private static String collection_name;
	
	private static String username;
	private static String password;
	//sa $RFV5tgb^YHN7ujm

	//"http://localhost:7979/solr/TITAN";
	//web_url_prefix for embedded images/documents and html
	private static String  web_url_prefix="";
	private static String translated_page_loc=null;
	public static String PROCESSED_FILES_FOLDER="PROCESSED_FILES";
	private static String WEB_CONTENT_ROOT_FOLDER="webapps/media/";
	public static String getWEB_CONTENT_ROOT_FOLDER() {
		return WEB_CONTENT_ROOT_FOLDER;
	}

	private static boolean extract_individual=false;

	public static boolean isExtract_individual() {
		return extract_individual;
	}


	private Collection _docs = new ArrayList();

	public static void main(String[] args) {
		try {

			if(args.length==0)
			{

				usage();
				return;
			}
			if (args.length > 0) {
				for (int i = 0; i < args.length; i++) {
					try {
						process(args[i]);
					} catch (Exception e) {

						e.printStackTrace();
					}
				}
			}

			dataImporterToNIU idxer=null;
			if(zookeeper_url!=null)
			{
				idxer = new dataImporterToNIU(zookeeper_url);
			}
			else if(collectionURL!=null)
			{
				idxer = new dataImporterToNIU(collectionURL);
			}

			idxer.doTikaDocuments(inputDir, extractDir, outputType,  web_url_prefix);
			//idxer.doSqlDocuments();

			idxer.endIndexing();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	

	public static void process(String arg) throws Exception {
		if (arg.equals("-?") || arg.equals("--help")) {

			usage();
		}else if (arg.equals("-v") || arg.equals("--verbose")) {
			Logger.getRootLogger().setLevel(Level.DEBUG);
		} 
		else if (arg.startsWith("--input-dir=")) {
			inputDir = new File(arg.substring("--input-dir=".length()));
		} 
		else if (arg.startsWith("--extract-dir=")) {
			extractDir = new File(arg.substring("--extract-dir=".length()));
		} 
		else if (arg.startsWith("--output_type=")) {
			outputType = arg.substring("--output_type=".length());
		}
		else if (arg.startsWith("--web_url_prefix=")) {
			web_url_prefix = arg.substring("--web_url_prefix=".length());
		}
		else if(arg.startsWith("--collection="))
		{
			collection_name=arg.substring("--collection=".length());
		}

		else if (arg.startsWith("--webapp-root-folder=")) {
			WEB_CONTENT_ROOT_FOLDER = arg.substring("--webapp-root-folder=".length());
		}
		else if(arg.startsWith("--zookeeper-url="))
		{
			zookeeper_url= arg.substring("--zookeeper-url=".length());
		}
		else if(arg.startsWith("--databaseHost="))
		{
			databasehost=arg.substring("--databaseHost=".length());
		}
		else if(arg.startsWith("--collectionURL="))
		{
			collectionURL=arg.substring("--collectionURL=".length());
		}
		else if(arg.startsWith("--extract-individual-to-destination"))
		{
			if(!arg.endsWith("false"))
			{
				extract_individual=true;
			}
		}
		else if(arg.startsWith("--username"))
		{
			username=arg.substring("--username=".length());
		}
		else if(arg.startsWith("--password"))
		{
			password=arg.substring("--password=".length());
		}
		else if(arg.startsWith("--translated_page_loc"))
		{
			translated_page_loc=arg.substring("--translated_page_loc".length());	
		}
		else if (arg.startsWith("--UNIQ_ID=")) {
			UNIQ_ID = arg.substring("--UNIQ_ID=".length());
		} 

	}
	static private void usage() {
		PrintStream out = System.out;

		out.println("usage: java -jar multimediaReader.jar $(input-dir) $(extract-dir) [option...] ");
		out.println();
		out.println("Options:");
		out.println("    -?  or --help          Print this usage message");
		out.println("    -v  or --verbose       Print debug level messages");
		out.println("     --input-dir=<dir> Specify input directory ");    
		out.println("    --extract-dir=<dir>    Extract all attachements and formatted html into current directory. \n           Default is same as parent folder");
		out.println("    --output_type=<type>    xml, html(extract embedded pictures), text. Default is text format");
		out.println("    --zookeeper-url=          solr zoo keepr URL, if  ingest to solr server is desired");
		out.println("    --collection=         solr collection name ");
		out.println("    --web_url_prefix=<type>   prefix web url where documents will be hosted in the web. ");
		out.println("                     embedded objects in the document and output html will be encode with prefix" );
		out.println("                     follow by relative path of input file");
		out.println("    --webapp-root-folder=  root folder of web app context that host media data. default webapps/media");
		out.println("                              used to figure out relative path of web address for embbed image and docs");
		out.println("    --extract-individual-to-destination=  go ahead extract individual page/unit to database or SOLR");
		out.println("                                           default is FALSE");
		out.println("    --collectionURL=http://localhost:8080/solr/TITAN");
		out.println("    --translated_page_loc=Translation  translation folder related to original doc. " +
								"\ntranlated_link will created if file exist");
		out.println("    --databaseHost=host  [default is dbstage] for NIU data to link wave file to content");
		out.println("    --UNIQ_ID=ID [default is XFIN_ID]");
		out.println("      EXAMPLE              --input-dir=/home/aweng/solrFinops/webapps/media --output_type=html" +
				" --web_url_prefix=http://localhost:7979 \n" +
				"--webapp-root-folder=/home/aweng/solrFinops/webapps/ \n" +
				"-zookeeper-url=10.1.70.231:2181 --collection=TITAN"+
				" --extract-individual-to-destination\n"+
				" --translated_page_loc=translation related to original doc");

		/*
		 * --input-dir=/opt/minimal/solrFinops/webapps/media/TITAN/ --output_type=html --web_url_prefix=http://10.1.3.42:7979  --webapp-root-folder=/opt/minimal/solrFinops/webapps/  --zookeeper-url=http://10.1.3.42:7979/solr/TITAN  --extract-individual-to-destination
		 * *
		 */
		out.println();
	}

	private dataImporterToNIU(String url) throws IOException, SolrServerException {
		// Create a multi-threaded communications channel to the Solr server.
		// Could be CommonsHttpSolrServer as well.
		//
		if(url!=null  && url.indexOf("http")<0)
		{
			_server =new CloudSolrServer(url);

			((CloudSolrServer)_server).setDefaultCollection(collection_name);


		}
		else
		{
			_server =new ConcurrentUpdateSolrServer(url, 100, 10);
		}




		try {
			

			//con = DriverManager.getConnection("jdbc:sqlserver://10.1.70.230:1433/"+databasename+"?"
			//		+ "user=xfin&password=c7c126ab81b48a666a75e0144550f2ee");
			if(collection_name!=null &&collection_name.toLowerCase().indexOf("niu")>=0)
			{
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
				log("Driver Loaded......");
				
				//String dburl="jdbc:sqlserver://10.1.70.230:1433;databaseName="+collection_name;

				String dburl="jdbc:sqlserver://"+databasehost+":1433;databaseName=XFIN";
				//String url="jdbc:mysql://" + DB_HOST + "/?"
				//		 + "user=" + USER + "&password=" + PASSWORD;

				//  + "useUnicode=true&characterEncoding=utf-8&user=" + USER + "&password=" + PASSWORD;

				log("connect to .."+dburl);
				con =
						DriverManager.getConnection(dburl, username, password);

				con.setReadOnly(true);

				niuDocStmt=con.prepareStatement("select * from [XFIN].[dbo].[NIU_CALL_2_00] where CALL_ID=?");

			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}


	}

	// Just a convenient place to wrap things up.
	private void endIndexing() throws IOException, SolrServerException {
		if (_docs.size() > 0) { // Are there any documents left over?
			_server.add(_docs, 60000); // Commit within 1 minutes
		}
		if(_server!=null)
		{
			_server.commit(); // Only needs to be done at the end,
		}
		// commitWithin should do the rest.
		// Could even be omitted
		// assuming commitWithin was specified.
		long endTime = System.currentTimeMillis();
		log("Total Time Taken: " + (endTime - _start) +
				" milliseconds to index " + _totalSql +
				" SQL rows and " + _totalTika + " documents");
	}


	private static void log(String msg) {
		if(  Logger.getLogger(dataImporterToNIU.class).isDebugEnabled())
			Logger.getLogger(dataImporterToNIU.class).debug(msg);

	}


	public static String encode(String url) throws UnsupportedEncodingException {
		/*
		try {
			url=new URI(url).getPath();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return url;
		 */
		String result = "";
		
		// Alternative: use Pattern.quote(File.separator)
		String pattern = Pattern.quote(System.getProperty("file.separator"));
		String[] urlElements = url.split(pattern);
		
		for (String s : urlElements) {
			if(s.startsWith("http://"))
			{
				result=s+File.separatorChar;
			}
			else
			{
			//.replaceAll("%3A", ":")
			
			result += URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20")
					.replaceAll("%3A", ":")
					//	.replaceAll("(", "%28").replaceAll("(", "%29").replaceAll("#", "%23").replaceAll("$", "%24")
					//escape number after %23, #
					//.replaceAll("%231", "%23\1").replaceAll("%232", "%23\2")
					+ File.separatorChar;
			}
		}
		if (result.length() > 0) {
			result = result.substring(0, result.length() - 1);
		}
		//go ahead replace window \\ file path with / for url
		if(File.separator.equals("\\"))
		{
			result=result.replaceAll(pattern, "/");
		}
		
		return result;

	}

	/**
	 * ***************************Tika processing here
	 */
	// Recursively traverse the filesystem, parsing everything found.
	public void doTikaDocuments(File root,  File output, String outputType, String web_url_prefix ) throws IOException, SolrServerException {


		// Simple loop for recursively indexing all the files
		// in the root directory passed in.
		for (File file :  root.listFiles()) {
			//PROCESSED_FILES folder contain those files that are processed
			if (file.isDirectory() ) {

				doTikaDocuments(file,  output, outputType, web_url_prefix);
				continue;
			}
			if(file.getPath().indexOf(PROCESSED_FILES_FOLDER)<0)
			{
				try{
				//start parsing the file
				MultimediaParser parser=new MultimediaParser();
			
				MultimediaParser.setWeb_url_prefix(web_url_prefix);
				String content="";
				try{
				  content= parser.processFile(file, output, outputType);
				}
				catch(Exception ex)
				{
					System.out.println(" catch exception in parse file: "+ex.getMessage());
				}
				// Just to show how much meta-data and what form it's in.
				//dumpMetadata(file.getCanonicalPath(), parser.getMetadata());
				//if content is empty or have empty body content
				if((!content.trim().equals("")) && content.indexOf("<body/>")<0)
				{

					//go ahead extract the content to the output folder.
					FileOutputStream out = new FileOutputStream(

							MultimediaParser.getCurrentOutputFolder()+File.separatorChar+

							file.getName()+"."+outputType);
					out.write(content.getBytes());
					out.close();

					log("saving "+file.getName()+"."+outputType+" to "+MultimediaParser.getCurrentOutputFolder());
				}
				if(zookeeper_url!=null || collectionURL!=null)
				{
					// Index just a couple of the meta-data fields.
					SolrInputDocument doc = new SolrInputDocument();


					doc.setField("metadata",  parser.getMetadata());
					// Crude way to get known meta-data fields.
					// Also possible to write a simple loop to examine all the
					// metadata returned and selectively index it and/or
					// just get a list of them.
					// One can also use the LucidWorks field mapping to
					// accomplish much the same thing.
					String author =  parser.getMetadata().get("Author");

					if (author != null ) {
						doc.addField("author", author);
						doc.addField("text", author);
					}
					else
					{

						author=parser.getMetadata().get("Last-Author");
						if (author != null ) {
							doc.addField("author", author);
							doc.addField("text", author);
						}
					}

					String contentType=parser.getMetadata().get("Application-Name");

					if(contentType==null)
					{
						contentType=parser.getMetadata().get("Content-Type");
					}
					//TODO: mediaformat have different name, look at result below. might want to merge it in the future

					if(contentType.toLowerCase().indexOf("powerpoint")>=0)
					{
						contentType="PowerPoint";
					}
					else if(contentType.toLowerCase().indexOf("excel")>=0)
					{
						contentType="Excel";
					}
					else if(contentType.toLowerCase().indexOf("word")>=0)
					{
						contentType="Word";
					}
					else if(contentType.toLowerCase().indexOf("spreadsheet")>=0)
					{
						contentType="spreadsheet";
					}
					else if(contentType.toLowerCase().indexOf("presentation")>=0)
					{
						contentType=" office presentation";
					}
					else if(contentType.toLowerCase().indexOf("wordprocessing")>0)
					{
						contentType="wordprocessing";
					}

					doc.addField("mediaFormat",contentType);


					String lastModifiedDate=parser.getMetadata().get("Last-Modified");
					if(lastModifiedDate!=null)
					{
						doc.setField("lastmodified",lastModifiedDate);
					}


					//  doc.addField("content", content);
					doc.addField("path", file.getAbsoluteFile());

					doc.addField("text", file.getAbsoluteFile());
					//	doc.addField("database_name",collection_name);
					List<String> cats=new ArrayList<String>();
					if(collection_name!=null)
					{
					if(collection_name.toLowerCase().contains("titan"))
					{
						cats.add("Investigation Data");
						cats.add("Case History Data");
						cats.add("DEA Special Operations Division (SOD) Data");
						doc.addField("source_nm", "TITAN");
					}
					else if(collection_name.toLowerCase().contains("niu"))
					{
						cats.add("Communication Intercepts");
						cats.add("Call Intercept Data");
						doc.addField("source_nm", "NIU");

					}
					else if(collection_name.toLowerCase().contains("nipr"))
					{
						//cats.add("Communication Intercepts");
						cats.add("NIPR Share Down Range Data");
						doc.addField("source_nm", "NIPR");
					}
					else
					{
						doc.addField("source_nm", collection_name);
					}
					doc.addField("categories_nm", cats);
					}

					
					String originalDoc=web_url_prefix+"/"+file.getPath().substring(WEB_CONTENT_ROOT_FOLDER.length());
					   if(web_url_prefix.endsWith(File.separator))
					   {
						   originalDoc=web_url_prefix+file.getPath().substring(WEB_CONTENT_ROOT_FOLDER.length());
					   }
					
					   originalDoc=encode(originalDoc);
					doc.addField("web_link", originalDoc);
					
					if(translated_page_loc!=null)
					{
						
						//if I am not a translated page, go ahead link translated_page to this location.
						if(file.getPath().indexOf("translated_page_loc")<0)
						{
						//construct translatedFilePath. 
						int lastPath=file.getPath().lastIndexOf(File.separator);
					   String translatedFilePath= file.getPath().substring(0,lastPath)+translated_page_loc+
							   file.getPath().substring(lastPath, file.getPath().lastIndexOf("."))+".docx";
					   log("translate path is"+translatedFilePath);
					   //check if tranlated page exist.
					   if (new File(translatedFilePath).exists())
					   {
						   
							String transDoc=web_url_prefix+"/"+translatedFilePath.substring(WEB_CONTENT_ROOT_FOLDER.length());
							   if(!WEB_CONTENT_ROOT_FOLDER.endsWith(File.separator))
							   {
								   transDoc=web_url_prefix+translatedFilePath.substring(WEB_CONTENT_ROOT_FOLDER.length());
							   }
							
							   transDoc=encode(transDoc);
							doc.addField("tranlated_link", transDoc);
						   
					   }
						}
						//link to orig page
						else
						{
							String origDoc= file.getPath().replace(translated_page_loc+File.separator, "").replace("pdf", "docx");
							log("this is translated page, trying to see if orig data exist in"+origDoc);
							if(new File(origDoc).exists())
							{
								doc.addField("original_link",  	originalDoc.replace(translated_page_loc+File.separator, "").replace("pdf", "docx"));
							}
							
						}
					}
					

					if(outputType.equals(MultimediaParser.HTML_TYPE))
					{


						if((!content.trim().equals("")) && (content.indexOf("<body/>")<0))
						{
							String currentOutput=MultimediaParser.getCurrentOutputFolder();


							String tmp=web_url_prefix;
							
							int idx=currentOutput.indexOf(WEB_CONTENT_ROOT_FOLDER);
							if(idx>=0 && WEB_CONTENT_ROOT_FOLDER.endsWith(File.separator))
							{
								tmp+="/";
							}
							if(idx>0)
							{
								tmp+=currentOutput.substring(idx);
							}

							//go the relative path inside the web content 
							else if(currentOutput.startsWith(WEB_CONTENT_ROOT_FOLDER))
							{
								tmp+=currentOutput.substring(WEB_CONTENT_ROOT_FOLDER.length());
							}
							tmp+=File.separatorChar+file.getName()+".html";
							
							//in window, encode it again,
							//if(File.separator.equals("\\"))
							//{
							tmp=encode(tmp);
							//}
							//System.out.print("tmp is "+tmp+" current output is "+MultimediaParser.getCurrentOutputFolder());
							/*
							

							tmp="<a href=\""+tmp+"\" target=\"_blank\"> HTML format</a><br></br>";
						tmp+="<a href=\""+ encode(web_url_prefix+File.separatorChar+file.getPath().substring(WEB_CONTENT_ROOT_FOLDER.length()));
						tmp+="\" target=\"_blank\"> Original Document</a>";
							 */

							//web_link, html representation of source document
							log("Web link "+tmp);

							doc.addField("html_link", tmp);
						}
					}
					//else 
					//{
						if(!content.trim().equals(""))
						{
							doc.addField("content", content);

							doc.addField("text", content);
							doc.addField("text_rev", content);
						}
					//}



					String xfinId=file.getCanonicalPath();
					if(xfinId.startsWith(WEB_CONTENT_ROOT_FOLDER))
					{
						xfinId.substring(WEB_CONTENT_ROOT_FOLDER.length());
					}
					xfinId=xfinId.replaceAll(" ", "");

					boolean extracted=false;
					if( extract_individual && MultimediaParser.getCurrentContents().size()>0)
					{

						for(Object page: MultimediaParser.getCurrentContents())
						{
							if(page instanceof PowerPointSlide)
							{
								extracted=true;
								PowerPointSlide slide=(PowerPointSlide)page;
								SolrInputDocument thePage=new SolrInputDocument();
								for(String key:doc.getFieldNames())
								{
									if(!key.equals("content"))
									{

										thePage.setField(key, doc.getFieldValues(key));
									}

								}
								//thePage.addField("categories_nm", cats);

								thePage.setField("page_number", slide.getSlideNumber());
								thePage.setField(UNIQ_ID, xfinId+"Slide"+slide.getSlideNumber());
								if(slide.getComments().size()>0)
								{
									thePage.setField("comments", slide.getComments());
									thePage.setField("text", slide.getComments());
								}
								if(slide.getNotes().size()>0)
								{
									thePage.setField("notes", slide.getNotes());
									thePage.setField("text", slide.getNotes());
								}
								if(slide.getImage_links().size()>0)
								{
									thePage.setField("image_links", slide.getImage_links());
								}
								thePage.setField("text", slide.getContent());
								thePage.setField("text_rev", slide.getContent());
								thePage.setField("content", slide.getContent());
								if(slide.getHeader()!=null)
								{
									thePage.setField("header", slide.getHeader());
									thePage.setField("text", slide.getHeader());
								}
								if(slide.getFooter()!=null)
								{
									thePage.setField("footer", slide.getFooter());
									thePage.setField("text", slide.getFooter());
								}
								_docs.add(thePage);
								++_totalTika;
								log("adding slide "+slide.getSlideNumber()+" to solr docs");

							}
							else if(page instanceof PDFData)
							{
								extracted=true;
								PDFData pdf=(PDFData)page;

								SolrInputDocument thePage=new SolrInputDocument();
								for(String key:doc.getFieldNames())
								{
									if(!key.equals("content"))
									{
										thePage.setField(key, doc.getFieldValues(key));
									}

								}
								//thePage.addField("categories_nm", cats);

								thePage.setField("page_number", pdf.getPageNumber());
								thePage.setField(UNIQ_ID, xfinId+"Slide"+pdf.getPageNumber());

								if(pdf.getImage_links().size()>0)
								{
									thePage.setField("image_links", pdf.getImage_links());
								}

								thePage.setField("content", pdf.getContent());
								thePage.setField("text", pdf.getContent());
								thePage.setField("text_rev", pdf.getContent());
								_docs.add(thePage);
								++_totalTika;
								log("adding page "+pdf.getPageNumber()+" to solr docs");

							}
						
							else if(page instanceof HashMap<?,?>)
							{
								SolrInputDocument thePage=new SolrInputDocument();
								for(String key:doc.getFieldNames())
								{
									if(!key.equals("content")&& !key.equals("metadata"))
									{

										thePage.setField(key, doc.getFieldValues(key));
									}

								}
								
							
								HashMap<String, String> nvpair=(HashMap<String, String>) page;
								thePage.setField(UNIQ_ID, xfinId+"Row"+nvpair.get("RowNumber"));
								
								for(String key:nvpair.keySet())
								{
									String value=nvpair.get(key);
									if(key!=null && value!=null)
									{
									thePage.setField(key, value);
									thePage.setField("text", value);
									thePage.setField("text_rev", value);
									}
									else
									{
										System.out.print("something is wrong , key or value is null");
									}
								}
							
								
								
								if(thePage.size()>0)
								{
								_docs.add(thePage);
								}
							
								
								extracted=true;

							}
							
							else if(page instanceof SortedMap<?, ?> )
							{

								SolrInputDocument thePage=new SolrInputDocument();
								for(String key:doc.getFieldNames())
								{
									if(!key.equals("content"))
									{

										thePage.setField(key, doc.getFieldValues(key));
									}

								}

								SortedMap<Point, Cell> excelSheet=(SortedMap<Point, Cell>)  page;
								if(excelSheet.size()>0)
								{
									int firstRow=excelSheet.firstKey().y;
									// Process Rows
									int currentRow = 0;
									int currentColumn = 0;

									for (Map.Entry<Point, Cell> entry : excelSheet.entrySet()) {


										while (currentRow < entry.getKey().y) {

											thePage.setField("row_number", currentRow+1);
											thePage.setField(UNIQ_ID, xfinId+"Row"+ currentRow+1);


											_docs.add(thePage);
											thePage=new SolrInputDocument();
											for(String key:doc.getFieldNames())
											{
												if(!key.equals("content") &&
														!key.equals("metadata"))
												{

													thePage.setField(key, doc.getFieldValues(key));
												}

											}
											++_totalTika;
											log("adding row "+ currentRow+1+" to solr docs");
											currentRow++;
											currentColumn = 0;
										}

										while (currentColumn < entry.getKey().x) {
											if(entry.getKey().y!=0)
											{
												Cell cell=excelSheet.get(new Point(entry.getKey().x, firstRow));
												if(cell!=null)
												{
													String key=cell.toString();
													String value=entry.getValue().toString();
													key=key.substring(value.indexOf(": ")+2).replaceAll("\"", "");
													if(key.contains("DT"))
													{
														key=key.replace("DT", "date");
													}
													value=value.substring(value.indexOf(": ")+2).replaceAll("\"", "");
												    if(value!=null && (!value.equals("")))
												    {
													//	System.out.println("key: "+key +"value: " +value);
													thePage.setField(key, value);
													thePage.addField("text",value);
													thePage.addField("text_rev", value);
												    }
												}
											}
											//  handler.endElement("td");
											//   handler.startElement("td");
											currentColumn++;
										}


									} 
									extracted=true;
								}
							}

	

						}
						//reset all current contents;
						MultimediaParser.getCurrentContents().clear();
					}
					//if haven't be extracted, go ahead add whole document
					if(	!extracted)
					{
						if((!content.trim().equals("")) &!doc.containsKey("content"))
						{
							doc.setField("content", content);
							doc.addField("text", content);
							doc.addField("text_rev", content);
						}
						doc.setField(UNIQ_ID, xfinId);
						//if it's NIU with wave file, query NIU NIU_CALL_DOC_MAP_ANNIE for wave file translation
						if(collection_name!=null && collection_name.toLowerCase().indexOf("niu")>=0 && contentType.indexOf("wav")>=0)
						{
							try{

								//get call id out of file path
								String CALL_ID=file.getPath().substring(file.getPath().lastIndexOf(File.separator));
								CALL_ID=CALL_ID.substring(CALL_ID.indexOf("__")+2, CALL_ID.indexOf(".wav"));
									//get the numbers portion of caller id
								 Pattern p = Pattern.compile("(\\d+)");
								    Matcher m = p.matcher(CALL_ID);
								    Integer j = null;
								    if (m.find()) {
								    	CALL_ID= m.group(1);
								    }
								    
								doc.setField("CALL_ID", CALL_ID);

								

								niuDocStmt.setString(1, CALL_ID);
								ResultSet contentRs=	niuDocStmt.executeQuery();
								//postion to name+_+type
								Map<Integer, String> dataTypeMap=new HashMap<Integer, String>();


								if(contentRs.next())
								{
									//haven't read the metadata yet, go ahead populate dataTypeMap, instead of reading table metadata for each row
									if(dataTypeMap.size()==0)
									{
										ResultSetMetaData rsm=  contentRs.getMetaData();
										for(int i=1; i<=rsm.getColumnCount(); i++)
										{
											//if it's varchar type with length >800, index it as reverse lookup type
											if(rsm.getPrecision(i)>=200  && (rsm.getColumnTypeName(i).indexOf("char")>=0))
											{
												dataTypeMap.put(i,   rsm.getColumnName(i)+";"+"LARGE_VARCHAR");
											}
											else
											{
												dataTypeMap.put(i,  rsm.getColumnName(i)+";"+rsm.getColumnTypeName(i));
											}
										}

									}
									for(int i=1; i<=dataTypeMap.size(); i++)
									{

										String columnName=dataTypeMap.get(i).substring(0, dataTypeMap.get(i).indexOf(";"));


										String columnType=dataTypeMap.get(i).substring(dataTypeMap.get(i).indexOf(";")+1);

										Object columnValue=null;
										try {
											columnValue = contentRs.getObject(i);
										} catch (SQLException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}


										if(columnType.indexOf("char")>=0|| columnType.equals("uniqueidentifier"))

										{
											//if column name ends with dt, and type is character. change
											//the column name to _date to avoid automatic binding of *_dt to dateTime
											//type in solr. this needed to avoid invalid date format when import
											if(columnName.contains("_dt"))
											{
												columnName=columnName.replace("dt", "date");

											}
											else if(columnName.contains("DT"))
											{
												columnName=columnName.replace("DT", "date");
											}

											doc.setField(columnName, columnValue);
											//add to text, to be index, not stored. see schema text field for more information.
											doc.addField("text",columnValue);

										}
										else if(columnType.equals("bigint"))
										{
											if (!columnName.equalsIgnoreCase("call_id"))
											{
												doc.setField(columnName+"_l", columnValue);
												doc.addField("text",columnValue);
											}
										}
										else if( columnType.indexOf("int")>=0)
										{

											//add the element as *_i so it will dynamically store and bind as integer type
											if (!columnName.equalsIgnoreCase("call_id"))
											{
												doc.setField(columnName+"_i",columnValue);
												doc.addField("text",columnValue);
											}

										}
										//go ahead copy it to text_rev, which will index tokens both normally and reverse for efficent leading wildcard queries.
										else if(columnType.equals("LARGE_VARCHAR"))
										{ //if column name ends with dt, and type is character. change
											//the column name to _date to avoid automatic binding of *_dt to dateTime
											//type in solr. this needed to avoid invalid date format when import
											if(columnName.contains("_dt"))
											{
												columnName=columnName.replace("dt", "date");

											}
											else if(columnName.contains("DT"))
											{
												columnName=columnName.replace("DT", "date");
											}
											doc.setField(columnName,columnValue);
											doc.addField("text",columnValue);
											doc.addField("text_rev",columnValue);
										}
										else if(columnType.equals("bit"))
										{
											//store bit columnType as boolean solr columnType 
											doc.addField(columnName+"_b", columnValue);
										}

										else if(columnType.equals("datetime"))
										{	//add it to date text to be facet
											doc.setField(columnName+"_dt", columnValue);
											doc.addField("date_text",columnValue);
										}
										else if(columnType.equals("numeric") ||columnType.equals("money") 
												|| columnType.equals("smallmoney") )
										{	//store as double type, since some of numeric type includes foat point, have to store as double type.
											doc.setField(columnName+"_d", columnValue);
											//only store money type as money_text. it's used for facet all money type
											if(columnType.equals("numeric"))
											{//add it to text, to be searchable
												doc.addField("text", columnValue);

											}
											else
											{
												//add it to money text to be facet
												doc.addField("money_text", columnValue);
											}
										}
										else if(columnType.equals("xml"))
										{
											doc.setField(columnName+"_t",columnValue);
											doc.addField("text",columnValue);

										}
										else
										{doc.addField("text",columnValue);
										doc.addField(columnName,columnValue);
										}
									}

								}
							}catch(Exception ex)
							{
								ex.printStackTrace();
							}

						}

						_docs.add(doc);
					}
					// Completely arbitrary, just batch up more than one document
					// for throughput!
					if (_docs.size() >= 20) {
						// Commit within 10 minutes.
						UpdateResponse resp = _server.add(_docs, 120000);
						if (resp.getStatus() != 0) {
							log("Some horrible error has occurred, status is: " +
									resp.getStatus());
						}
						//_server.commit();
						_docs.clear();
					}

				}
				}catch(Exception ex)
				{
					ex.printStackTrace();
				}
			}
		}

	}
}