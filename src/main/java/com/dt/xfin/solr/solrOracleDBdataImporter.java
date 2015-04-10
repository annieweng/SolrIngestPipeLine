package com.dt.xfin.solr;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.metadata.Metadata;

/* this class will connect to a oracle database and import
 * either a table or database instance into a solr server.
 * this class assume some generic metadata in solr server.
 * for example, date_text will be map to all database object that are datetime
 * type. money_text will be map to all database object that is money type.
 * all non-floating point type will be added to "text" field in solr.
 * this enable a global facet of money, date value, and global
 * search of all data using default "text" field.
 * 
 * add following data type to solr/config/schema.xml
 *
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

 * 
 */
public class solrOracleDBdataImporter extends Thread {
	private SolrServer _server = null;
	private long _start = System.currentTimeMillis();
	private static boolean MULTI_THREAD = false;
	private static File inputDir;
	private static File extractDir;
	//global unique Id defined in solr schema to ensure
	// each entries in solr are uniquely.
	private static String UNIQ_ID = "UNIQ_ID";
	//SID or service instance name of oracle database
	private static String SID = "xdata";


	private static String startTable = null;
	private int _totalSql = 0;
	//if using solr cloud, zookeeper's url
	private static String zookeeper_url = null;

	//collection name of solr instance. in general we will
	//want collectionName to reflect database tablespace name
	public static String collectionName;

	public static String databasename;
	public static String databasehost = "localhost";
	//SOLR collection URL
	public static String collectionURL = null;
	// "http://localhost:7979/solr/TITAN";
	// web_url_prefix for embedded images/documents and html
	private static String web_url_prefix = "";
	public static String PROCESSED_FILES_FOLDER = "PROCESSED_FILES";
	//default all multimedia file to webapps/media
	private static String WEB_CONTENT_ROOT_FOLDER = "webapps/media/";
	public static boolean deltaImport = false;
	public static String tableName = null;
	private static String username;

	private static String password;
	public static String tableCondition = null;
	//metadata table to query all tables in the database
	private static  String allTableName="all_tables";

	public static String getWEB_CONTENT_ROOT_FOLDER() {
		return WEB_CONTENT_ROOT_FOLDER;
	}

	//extract individual records in a document.
	//for example, one row of excel spread sheet will be written
	//to a seperate solr record. 
	private static boolean extract_individual = false;

	public static boolean isExtract_individual() {
		return extract_individual;
	}

	public void run() {
		try {
			//multiple thread support, start at different time
			//to avoid over run of solr server.  
			//this might not be needed with newer solr server.
			if (MULTI_THREAD) {
				if (this.getName().contains("-1")) {
					try {
						Thread.sleep(60000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (this.getName().contains("-2")) {
					try {
						Thread.sleep(120000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (this.getName().contains("-3")) {
					try {
						Thread.sleep(180000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (this.getName().contains("-4")) {
					try {
						Thread.sleep(260000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (this.getName().contains("-5")) {
					try {
						Thread.sleep(340000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			doSqlDocuments();

			endIndexing();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Collection _docs = new ArrayList();
	public String tableQuery = null;

	public static void main(String[] args) {
		try {

			if (args.length == 0) {

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

			solrOracleDBdataImporter idxer = null;
			if (zookeeper_url != null) {
				idxer = new solrOracleDBdataImporter(zookeeper_url);
			} else if (collectionURL != null) {
				idxer = new solrOracleDBdataImporter(collectionURL);
			}
			if (tableName != null) {
				// if the table name has and clause, go ahead put tablename is
				// quote
				if (tableName.contains(" and")) {
					tableCondition = tableName.substring(tableName
							.indexOf(" and") + 5);
					tableName = "'"
							+ tableName.substring(0, tableName.indexOf(" and"))
							.trim() + "'";

				} else {
					tableName = "'" + tableName + "'";
				}
				if (idxer != null) {
					idxer.tableQuery = " table_name like " + tableName;

					idxer.start();
				}

			} else {
				if (idxer != null) {
					if (MULTI_THREAD) {
						idxer.tableQuery = " table_name<='c'";
					}
					idxer.start();
				}

				if (MULTI_THREAD) {
					solrOracleDBdataImporter idxer2 = null;
					if (zookeeper_url != null) {
						idxer2 = new solrOracleDBdataImporter(zookeeper_url);
					} else if (collectionURL != null) {
						idxer2 = new solrOracleDBdataImporter(collectionURL);
					}
					if (idxer2 != null) {
						idxer2.tableQuery = " table_name>'c' AND table_name<'h'";

						idxer2.start();
					}

					solrOracleDBdataImporter idxer3 = null;
					if (zookeeper_url != null) {
						idxer3 = new solrOracleDBdataImporter(zookeeper_url);
					} else if (collectionURL != null) {
						idxer3 = new solrOracleDBdataImporter(collectionURL);
					}
					if (idxer3 != null) {
						idxer3.tableQuery = " table_name>='h' AND table_name<'k'";
						idxer3.start();
					}

					solrOracleDBdataImporter idxer4 = null;
					if (zookeeper_url != null) {
						idxer4 = new solrOracleDBdataImporter(zookeeper_url);
					} else if (collectionURL != null) {
						idxer4 = new solrOracleDBdataImporter(collectionURL);
					}
					if (idxer4 != null) {
						idxer4.tableQuery = " table_name>='k' AND table_name<'s'";
						idxer4.start();
					}

					solrOracleDBdataImporter idxer5 = null;
					if (zookeeper_url != null) {
						idxer5 = new solrOracleDBdataImporter(zookeeper_url);
					} else if (collectionURL != null) {
						idxer5 = new solrOracleDBdataImporter(collectionURL);
					}
					if (idxer5 != null) {
						idxer5.tableQuery = " table_name>='s' AND table_name<'u'";
						idxer5.start();
					}
					solrOracleDBdataImporter idxer6 = null;
					if (zookeeper_url != null) {
						idxer6 = new solrOracleDBdataImporter(zookeeper_url);
					} else if (collectionURL != null) {
						idxer6 = new solrOracleDBdataImporter(collectionURL);
					}
					if (idxer6 != null) {
						idxer6.tableQuery = " table_name>='u' ";
						idxer6.start();
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void process(String arg) throws Exception {
		if (arg.equals("-?") || arg.equals("--help")) {

			usage();
		} else if (arg.equals("-v") || arg.equals("--verbose")) {
			Logger.getRootLogger().setLevel(Level.DEBUG);
		} else if (arg.startsWith("--databaseHost=")) {
			databasehost = arg.substring("--databaseHost=".length());
		} else if (arg.startsWith("--databaseName=")) {
			databasename = arg.substring("--databaseName=".length());
		} else if (arg.startsWith("--UNIQ_ID=")) {
			UNIQ_ID = arg.substring("--UNIQ_ID=".length());
		} else if (arg.startsWith("--collectionURL=")) {
			collectionURL = arg.substring("--collectionURL=".length());
		} else if (arg.startsWith("--collectionName=")) {
			collectionName = arg.substring("--collectionName=".length());
		} else if (arg.startsWith("--zookeeper-url=")) {
			zookeeper_url = arg.substring("--zookeeper-url=".length());
		} 

		else if (arg.startsWith("--tableName=")) {
			tableName = arg.substring("--tableName=".length());
		} else if (arg.startsWith("--startTable=")) {
			startTable = arg.substring("--startTable=".length());
		} else if (arg.startsWith("--deltaImport=")) {
			if (arg.toLowerCase().equals("--deltaImport=true")) {
				deltaImport = true;
				System.out
				.print("Delta Import is true, will only import if table doesn't exist in solr");
			}
		} else if (arg.startsWith("--username")) {
			username = arg.substring("--username=".length());
		} else if (arg.startsWith("--password")) {
			password = arg.substring("--password=".length());
		} else if (arg.startsWith("--SID")) {
			SID = arg.substring("--SID=".length());
		} else if (arg.startsWith("--MULTI_THREAD")) {
			if (arg.substring("--MULTI_THREAD=".length()).toLowerCase() == "true")
				MULTI_THREAD = true;
		}
		else if (arg.startsWith("--allTableName")){
			allTableName=arg.substring("--allTableName".length());
		}

	}

	static private void usage() {
		PrintStream out = System.out;

		out.println("usage: java -jar solrOracleDBdataImporter.jar $(input-dir) $(extract-dir) [option...] ");
		out.println();
		out.println("Options:");
		out.println("    -?  or --help          Print this usage message");
		out.println("    -v  or --verbose       Print debug level messages");

		out.println("    --zookeeper-url            solr URL, if  ingest to solr server is desired");

		out.println("    --startTable table to start with for import");
		out.println("    --databaseName=TITAN");
		out.println("    --databaseHost=host  [default is localhost]");
		out.println("    --collectionURL=http://localhost:8080/solr/TITAN");
		out.println("    --tableName=KBB_TRANSACTION_2_10  only import this table");
		out.println("    --UNIQ_ID=ID [default is XFIN_ID]");
		out.println("    --username ");
		out.println("    --password ");
		out.println("    --SID ");
		out.println("    --MULTI_THREAD=TRUE");
		out.println("    --allTableName=[dba_tables|all_tables] oracle table that show tables to import within databaseName");

		out.println("      EXAMPLE             "
				+ "[--zookeeper-url=10.1.70.231:2181 --collectionName TITAN] "
				+ "or multi Thread  [--collectionURL=http://localhost:8080/solr/TITAN]   --databaseName=\n");


		out.println();
	}

	private solrOracleDBdataImporter(String url) throws IOException,
	SolrServerException {
		// Create a multi-threaded communications channel to the Solr server.
		// Could be CommonsHttpSolrServer as well.
		// it's zookeepr url, use cloudSolrServer
		if (url != null && url.indexOf("http") < 0) {
			if (collectionName == null) {
				collectionName = databasename;
			}
			_server = new CloudSolrServer(url);

			((CloudSolrServer) _server).setDefaultCollection(collectionName);
			((CloudSolrServer) _server).setZkConnectTimeout(30000);
			((CloudSolrServer) _server).setZkClientTimeout(30000);

			log("connecting to  zookeeper CloudSolrServer: " + url
					+ " with collection : " + collectionName);

		} else {
			// create a concurent update solr server with 100 queue size, with
			// 10 concurrent thread
			_server = new ConcurrentUpdateSolrServer(url, 100, 10);

		}

	}

	// Just a convenient place to wrap things up.
	private void endIndexing() throws IOException, SolrServerException {
		if (_docs.size() > 0) { // Are there any documents left over?
			_server.add(_docs, 120000); // Commit within 2 minutes
		}
		if (_server != null) {
			_server.commit(); // Only needs to be done at the end,
		}
		// commitWithin should do the rest.
		// Could even be omitted
		// assuming commitWithin was specified.
		long endTime = System.currentTimeMillis();
		System.out.println("in thread" + this.getName() + "Total Time Taken: "
				+ (endTime - _start) + " milliseconds to index " + _totalSql
				+ " SQL rows and ");
	}

	private static void log(String msg) {
		// if( Logger.getLogger(solrDBdataImporter.class).isDebugEnabled())
		// Logger.getLogger(solrDBdataImporter.class).debug(msg);
		System.out.println(msg);
	}

	public static String encode(String url) throws UnsupportedEncodingException {
		String[] urlElements = url.split(File.separator);
		String result = "";
		for (String s : urlElements) {
			result += URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20")
					.replaceAll("%3A", ":")
					+ File.separatorChar;
		}
		if (result.length() > 0) {
			result = result.substring(0, result.length() - 1);
		}
		return result;
	}

	// Just to show all the metadata that's available.
	private void dumpMetadata(String fileName, Metadata metadata) {
		log("Dumping metadata for file: " + fileName);
		for (String name : metadata.names()) {
			log(name + ":" + metadata.get(name));
		}
		log("\n\n");
	}

	/**
	 * ***************************SQL processing here
	 */
	private void doSqlDocuments() throws SQLException {
		Connection con = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			log("Driver Loaded......");

			String url = "jdbc:oracle:thin:@" + databasehost + ":1521:" + SID;

			// String url="jdbc:mysql://" + DB_HOST + "/?"
			// + "user=" + USER + "&password=" + PASSWORD;

			// + "useUnicode=true&characterEncoding=utf-8&user=" + USER +
			// "&password=" + PASSWORD;

			log("connect to .." + url);
			con =

					DriverManager.getConnection(url, username, password);
			con.setReadOnly(true);

			Statement st = con.createStatement();
			//go ahead select all tables that below to databasename schema
			String query = "select table_name from "+allTableName+" where owner='"
					+ databasename + "'";

			if (tableQuery != null) {
				query += " AND " + tableQuery;
			}

			query += " order by table_name";
			System.out.println("query is " + query + " at thread"
					+ this.getName());
			// get distinct tables from the database
			ResultSet tableRs = st.executeQuery(query);
			// go ahead get the table content
			// PreparedStatement
			// metaStmt=con.prepareStatement("select position, column_nm, data_type, vendor from KBBDENORM.dbo.KBB_SCHEMA_DICTIONARY where"
			// +
			// " table_nm= ? and database_nm="+databasename);
			// go ahead get the table content
			boolean foundStartTable = false;

			if (startTable == null) {
				foundStartTable = true;
			}

			while (tableRs.next()) {

				String table_name = tableRs.getString("table_name");

				// still haven't find start table yet, go ahead skip to next
				// table
				if (!foundStartTable) {

					if (startTable.equalsIgnoreCase(table_name)) {
						System.out.print(" starting importing db from table "
								+ startTable);
						foundStartTable = true;
					} else {
						continue;
					}

				}

				if (deltaImport) {
					SolrQuery squery = new SolrQuery();
					squery.setQuery("table_nm:" + table_name);

					QueryResponse rsp = _server.query(squery);
					// found some entry of number
					if (rsp.getResults().getNumFound() > 0) {
						Statement tablestmt = con.createStatement();
						ResultSet countRs = tablestmt
								.executeQuery("select count(*) from "
										+ databasename + "." + table_name + "");
						if (countRs.next()) {
							long count = countRs.getLong(1);
							// table already populated, skip to next table
							if (rsp.getResults().getNumFound() >= count) {
								log(" table.... "
										+ table_name
										+ " already popuplated. skip to next table");
								continue;
							}
						}

					}

				}

				log("getting table.... " + table_name);
				Statement tablestmt = con.createStatement();
				tablestmt = con.createStatement(
						ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_READ_ONLY);
				try {
					String q = "select * from " + databasename + "."
							+ table_name + "";
					if (tableCondition != null) {
						q += " where " + tableCondition;
					}
					System.out.println("query: " + q);
					ResultSet contentRs = tablestmt.executeQuery(q);

					// postion to name+_+type
					Map<Integer, String> dataTypeMap = new HashMap<Integer, String>();

					while (contentRs.next()) {
						// haven't read the metadata yet, go ahead populate
						// dataTypeMap, instead of reading table metadata for
						// each row
						if (dataTypeMap.size() == 0) {
							ResultSetMetaData rsm = contentRs.getMetaData();
							for (int i = 1; i <= rsm.getColumnCount(); i++) {
								// if it's varchar type with length >800, index
								// it as reverse lookup type
								if (rsm.getPrecision(i) >= 200
										&& (rsm.getColumnTypeName(i)
												.toLowerCase().indexOf("char") >= 0)) {
									dataTypeMap.put(i, rsm.getColumnName(i)
											+ ";" + "LARGE_VARCHAR");
								} else {
									dataTypeMap.put(i, rsm.getColumnName(i)
											+ ";" + rsm.getColumnTypeName(i));
								}
							}

						}
						SolrInputDocument doc = new SolrInputDocument();
						List<String> category = new ArrayList<String>();

						doc.addField("database_nm", databasename);

						doc.addField("table_nm", table_name);

						String xfin_id = null;

						for (int i = 1; i <= dataTypeMap.size(); i++) {

							String columnName = dataTypeMap.get(i).substring(0,
									dataTypeMap.get(i).indexOf(";"));
							// there are case where column_name is table_nm, to
							// avoid conflict with metatadata table_nm
							// rename column name table_nm to table_nm_s(for
							// string type)
							if (columnName.equalsIgnoreCase("table_nm")) {
								columnName = columnName + "_s";
							}

							String columnType = dataTypeMap.get(i).substring(
									dataTypeMap.get(i).indexOf(";") + 1);

							// if column ends with dt, solr date type, and the
							// column type is not dateTime. go ahead
							// rename it to date, so the column will not be map
							// to date type in solr. without this
							// logic, validation of date format will fail in
							// solr and resulting record not being inserted
							if ((columnName.endsWith("_dt") || columnName
									.endsWith("_dts"))
									&& (columnType.toLowerCase()
											.indexOf("time") < 0 || columnType
											.toLowerCase().indexOf("date") < 0)) {
								columnName = columnName.replaceAll("_dt",
										"_date").replaceAll("_tdt", "_date");
							}

							Object columnValue = contentRs.getObject(i);
							if(columnValue==null ||String.valueOf(columnValue).trim().equals("") )
							{
								continue;
							}
							if (columnName.equals(UNIQ_ID)) {
								xfin_id = (String) columnValue;
							}
							if (columnType.indexOf("char") >= 0
									|| columnType.equals("uniqueidentifier"))

							{

								doc.addField(columnName, columnValue);
								// add to text, to be index, not stored. see
								// schema text field for more information.
								doc.addField("text", columnValue);

							} else if (columnType.equals("bigint")) {
								columnValue = String.valueOf(columnValue);

								doc.addField(columnName + "_l", columnValue);
								doc.addField("text", columnValue);
							} else if (columnType.indexOf("NUMBER") >= 0) {
								columnValue = String.valueOf(columnValue);
								// add the element as *_i so it will dynamically
								// store and bind as integer type

								String decimalPattern = "([-0-9]*)\\.([0-9]*)";  

								boolean match = Pattern.matches(decimalPattern, String.valueOf(columnValue));
								//it's float point value, don't add it to text
								if(match)
								{
									doc.addField(columnName , columnValue);
								}
								else
								{

									doc.addField(columnName + "_i", columnValue);
									doc.addField("text", columnValue);
								}

							}
							// go ahead copy it to text_rev, which will index
							// tokens both normally and reverse for efficent
							// leading wildcard queries.
							else if (columnType.equals("LARGE_VARCHAR")) {
								doc.addField(columnName, columnValue);
								doc.addField("text", columnValue);
								doc.addField("text_rev", columnValue);
							} else if (columnType.equals("bit")) {
								// store bit columnType as boolean solr
								// columnType
								doc.addField(columnName + "_b", columnValue);
							}

							else if (columnType.toLowerCase().indexOf("time") >= 0
									|| columnType.toLowerCase().indexOf("date") >= 0) { // add
								// it
								// to
								// date
								// text
								// to
								// be
								// facet
								doc.addField(columnName + "_tdt", columnValue);
								doc.addField("date_text", columnValue);
							} else if (columnType.toLowerCase()
									.indexOf("money") >= 0) {
								columnValue = String.valueOf(columnValue);

								doc.addField(columnName, columnValue);

								// add it to money text to be facet
								doc.addField("money_text", columnValue);

							} else if (columnType.toLowerCase().indexOf("xml") >= 0) {
								doc.addField(columnName + "_t", columnValue);
								doc.addField("text", columnValue);

							}
							// don't add float type to text field
							else if (columnType.toLowerCase().indexOf("float") >= 0) {
								columnValue = String.valueOf(columnValue);

								doc.addField(columnName, columnValue);
							} else {
								doc.addField("text", columnValue);
								doc.addField(columnName, columnValue);
							}

						}
						if (xfin_id == null) {

							String uuid = UUID.randomUUID().toString();

							doc.addField(UNIQ_ID, uuid);
							doc.addField("text", uuid);
						}

						_docs.add(doc);
						++_totalSql;

						// Completely arbitrary, just batch up more than one
						// document for throughput!
						if (_docs.size() > 100) {
							// Commit within 2 minutes.
							UpdateResponse resp = _server.add(_docs, 120000);
							if (resp.getStatus() != 0) {
								log("Some horrible error has occurred, status is: "
										+ resp.getStatus());
							} else {
								// log("soft commit "+_totalSql+" with in 2 minutes");

							}
							_docs.clear();
						}
						if (_totalSql % 100000 == 0) {
							System.out.println("Total Time Taken: "
									+ (System.currentTimeMillis() - _start)
									/ 1000 / 60 + " minutes to index "
									+ _totalSql + " SQL rows and ");
						}

					}
					contentRs.close();

				} catch (Exception ex) {
					System.out.println("exception found. current table is "
							+ table_name);
					ex.printStackTrace();
				}
			}
		} catch (Exception ex) {

			ex.printStackTrace();
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}
}