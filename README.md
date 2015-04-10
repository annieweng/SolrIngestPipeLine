Author: Annie Weng

this package extend basic solr dataImporter and tika/cell functions and provide much more comprehensive
functionality and capability to support large scale solr ingest pipeline.

1. it provide generic schema definition to allow facet and query of data across all data source. for example,
it will record database_nm, table_nm, date_text, money_text for each row of data in database. 
date_text will be mapped to any database field that has datetime/date as data type. by doing so, 
user are able to facet/filter data base on date_text without knowing exactly what the original database field name is.

2. it generate metadata fields to allow UI to refer back to original
source data on each record. It generate a formatted html/text ouptut for each files that it ingest to allow user to
view data in html/text format regardless of original data format.

3. it provide user a option to parse and store a row/page of data as individual record, instead of entire document
that is provided by cell/tika. each record will have metadata of page, row, tab name etc.

4. it support additional pdf Jbig2Image format that PDFBox doesn't support. this format is commonly 
used by scanned document.

5. corresponding front end User interface is available to download at
   https://github.com/annieweng/SorlJS.git

 
 following schema fields should be added to your solr
solr schema.xml

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

recommand media file setup.
put all media files in the same server as your web container.
create a web context url to refer to you media folder in your web server.





This package product 3 different executable jar files.
1.com.dt.xfin.solr.dataImporter- utility to import multimedia folder to solr
2. com.dt.xfin.solr.solrDBDataImporter- utility to import Microsoft SQL server database to solr
3. com.dt.xfin.solr.solrOracleDBdataImport- utility to import oracle database to solr

change main class in pom.xml to provide desired executable.
see indiviual source file for more detail of 
 is create to import any multimedia format that is supported by Tika automatically into SOLR or database.
These are the general steps to do before starting importing data.
1. put source folder in web container's webapp/$(webContext) folder.
2. run java -jar multimediaReader-1.3.jar as following
the processed file(.html with embedded file) will be under PROCESSED_FILES unless specified.


example setup for ingesting multimedia files to solr

create a media.xml to point to directory where multimedia directory are located.
at this case, the input directory is H:\media in a window file system.

<?xml version="1.0" encoding="UTF-8"?>
<Context path="/media"
     docBase="H:\media\"
     allowlinking="true"
     crosscontext="true"
     debug="0"
     antiResourceLocking="false"
     privileged="true">
 
</Context>

place this file in $CATALINA_HOME/Catalina/localhost/ directory. this enable web container to 
server all media files in context of http://localhost:8080/media url.

Enter following command ingest all data in input_dir to solr collection
java -jar dataImporter-1.5.jar --input-dir=H:\media --output_type=html  --web_url_prefix=http://localhost:8080 --collectionURL=http://localhost:8983/solr/DOCS --extract-individual-to-destination=true  --UNIQ_ID=UNIQ_ID --webapp-root-folder=H:\


