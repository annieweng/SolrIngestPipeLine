/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tika.parser.microsoft;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ddf.EscherBSERecord;
import org.apache.poi.ddf.EscherContainerRecord;
import org.apache.poi.ddf.EscherRecord;
import org.apache.poi.hslf.HSLFSlideShow;
import org.apache.poi.hslf.model.*;
import org.apache.poi.hslf.usermodel.ObjectData;
import org.apache.poi.hslf.usermodel.PictureData;
import org.apache.poi.hslf.usermodel.SlideShow;
import org.apache.poi.poifs.filesystem.DirectoryNode;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.dt.xfin.data.PowerPointSlide;
import com.dt.xfin.parser.MultimediaParser;
import com.dt.xfin.solr.dataImporter;
import org.apache.log4j.Logger;

public class HSLFExtractor extends AbstractPOIFSExtractor {
	public HSLFExtractor(ParseContext context) {
		super(context);
	}

	protected void parse(
			NPOIFSFileSystem filesystem, XHTMLContentHandler xhtml)
					throws IOException, SAXException, TikaException {
		parse(filesystem.getRoot(), xhtml);
	}

	protected void parse(
			DirectoryNode root, XHTMLContentHandler xhtml)
					throws IOException, SAXException, TikaException {
		HSLFSlideShow ss = new HSLFSlideShow(root);
		SlideShow _show = new SlideShow(ss);
		Slide[] _slides = _show.getSlides();
		
		xhtml.startElement("div", "class", "slideShow");

		/* Iterate over slides and extract text */
		for( Slide slide : _slides ) {
			PowerPointSlide pt=new PowerPointSlide();
			if(dataImporter.isExtract_individual())
			{   pt.setSlideNumber(slide.getSlideNumber());
			    
			  
			}
			xhtml.startElement("div", "class", "slide");


			
			// Slide header, if present
			HeadersFooters hf = slide.getHeadersFooters();
			
			if (hf != null && hf.isHeaderVisible() && hf.getHeaderText() != null) {
				xhtml.startElement("p", "class", "slide-header");
				xhtml.startElement("h1");
				xhtml.characters( hf.getHeaderText() );
				xhtml.endElement("h1");
				xhtml.endElement("p");
				if(dataImporter.isExtract_individual())
				{   pt.setHeader(hf.getHeaderText());
				    				  
				}
			}
			
			


			// Slide master, if present
			// TODO: re-enable this once we fix TIKA-712
			/*
			MasterSheet master = slide.getMasterSheet();
			
			if(master != null) {
				xhtml.startElement("p", "class", "slide-master-content");
				textRunsToText(xhtml, master.getTextRuns(), true );
				xhtml.endElement("p");
			}
			*/
			
			//now retrieve pictures containes in the first slide and save them on disk
			Shape[] sh = slide.getShapes();
			int imageNumber=0;
			if(sh.length>0)
			{
				xhtml.startElement("div", "class", "images");
				
				for (int i = 0; i < sh.length; i++){
					if (sh[i] instanceof Picture){
						//construct a name for image
						String name="slide_"+slide.getSlideNumber();
						if(imageNumber>0)
						{
							name=name+"_"+imageNumber;
						}
						Picture pict = (Picture)sh[i];
						PictureData pictData = pict.getPictureData();
						if(pictData==null)
						{
							continue;
						}
						byte[] data = pictData.getData();
						int type = pictData.getType();
						if (type == Picture.JPEG){

							name=name+".jpg";          
							imageNumber++;
						} else if (type == Picture.PNG){

							name=name+".png";            	
							imageNumber++;

						}
						else if(type==Picture.EMF){
							name=name+".emf";
							imageNumber++;
						}
						else if(type==Picture.WMF){
							name=name+".wmf";
							imageNumber++;
						}
						else if(type==Picture.PICT){
							name=name+".pict";
							imageNumber++;
						}
						
						//go ahead write the image to current output folder
						FileOutputStream out = new FileOutputStream(MultimediaParser.getCurrentOutputFolder()+File.separatorChar+name);
						out.write(data);
						out.close();
                                                Logger.getLogger(HSLFExtractor.class).debug("saving image  "+name+" to "+MultimediaParser.getCurrentOutputFolder());
						
						
						
						name=MultimediaParser.getUrlOutputFileName(name);
                                                name= dataImporter.encode(name);
							
						
						
								
					
						 AttributesImpl attributes = new AttributesImpl();
					        attributes.addAttribute("", "src", "src", "CDATA", name);
					        attributes.addAttribute("", "width", "width", "CDATA", "250");
					  
					
					        xhtml.startElement("img", attributes);
						xhtml.endElement("img");
						if(dataImporter.isExtract_individual())
						{
						
						    
						  
								pt.getImage_links().add( name);
							
						}
						

					}
				}
				xhtml.endElement("div");
			}
			
			// Slide text
			{
				xhtml.startElement("p", "class", "slide-content");

			 String txt=	textRunsToText(xhtml, slide.getTextRuns(), false );
				if(dataImporter.isExtract_individual())
				{
					pt.setContent(txt);
				}

				xhtml.endElement("p");
			}

			// Slide footer, if present
			if (hf != null && hf.isFooterVisible() && hf.getFooterText() != null) {
				xhtml.startElement("p", "class", "slide-footer");

				xhtml.characters( hf.getFooterText() );
				if(dataImporter.isExtract_individual())
				{
					pt.setFooter(hf.getFooterText());
				}

				xhtml.endElement("p");
			}

			// Comments, if present
			for( Comment comment : slide.getComments() ) {
				xhtml.startElement("p", "class", "slide-comment");
				
				String txt="";
				if (comment.getAuthor() != null) {
					xhtml.startElement("b");
					xhtml.characters( comment.getAuthor() );
					xhtml.endElement("b");
					
					txt=" Author: "+comment.getAuthor()+"\n";
					if (comment.getText() != null) {
						xhtml.characters( " - ");
					}
				}
				if (comment.getText() != null) {
					xhtml.characters( comment.getText() );
					txt+=" comment: "+comment.getText()+"\n";
				}
				xhtml.endElement("p");
				if(dataImporter.isExtract_individual())
				{
					pt.getComments().add(txt);
				}
			}


			xhtml.startElement("div", "class", "slideNotes");



			Notes notes = slide.getNotesSheet();
			if (notes != null) {

					String txt="";

				// Repeat the Notes header, if set
				if (hf != null && hf.isHeaderVisible() && hf.getHeaderText() != null) {
					xhtml.startElement("p", "class", "slide-note-header");
					xhtml.characters( hf.getHeaderText() );
					xhtml.endElement("p");
					txt=hf.getHeaderText() +"/n";

				}

				// Notes text
				txt+=textRunsToText(xhtml, notes.getTextRuns(), false);

				// Repeat the notes footer, if set
				if (hf != null && hf.isFooterVisible() && hf.getFooterText() != null) {
					xhtml.startElement("p", "class", "slide-note-footer");
					xhtml.characters( hf.getFooterText() );
					txt+=hf.getFooterText();
					xhtml.endElement("p");
				}
				
				if(dataImporter.isExtract_individual())
				{
					pt.getNotes().add(txt);
				}

			}

			// Now any embedded resources
			//         handleSlideEmbeddedResources(slide, xhtml);

			// TODO Find the Notes for this slide and extract inline

			// Slide complete
			xhtml.endElement("div");
			
			xhtml.startElement("hr");
			xhtml.endElement("hr");
			if(dataImporter.isExtract_individual())
			{
				MultimediaParser.getCurrentContents().add(pt);
			}
		}

		// All slides done
		xhtml.endElement("div");

		/* notes */





		//   handleSlideEmbeddedPictures(_show, xhtml);

		xhtml.endElement("div");


	}

	private String textRunsToText(XHTMLContentHandler xhtml, TextRun[] runs, boolean isMaster) throws SAXException {
		String txt="";
		if (runs==null) {
			return txt;
		}

		for (TextRun run : runs) {
			if (run != null) {
		
				// Avoid boiler-plate text on the master slide (0
				// = TextHeaderAtom.TITLE_TYPE, 1 = TextHeaderAtom.BODY_TYPE):
				if (!isMaster || (run.getRunType() != 0 && run.getRunType() != 1)) {
					
					txt+="<pre>"+run.getText()+"</pre></br>";
					//preserve the character
					xhtml.startElement("pre");
					xhtml.characters(run.getText());
					xhtml.endElement("pre");
					xhtml.startElement("br");
					xhtml.endElement("br");
				}
			}
		}
		return txt;
	}
	

	private void handleSlideEmbeddedPictures(SlideShow slideshow, XHTMLContentHandler xhtml)
			throws TikaException, SAXException, IOException {
		xhtml.startElement("div", "class", "pictures");
		for (PictureData pic : slideshow.getPictureData()) {
			String mediaType = null;

			switch (pic.getType()) {
			case Picture.EMF:
				mediaType = "application/x-emf";
				break;
			case Picture.JPEG:
				mediaType = "image/jpeg";
				break;
			case Picture.PNG:
				mediaType = "image/png";
				break;
			case Picture.WMF:
				mediaType = "application/x-msmetafile";
				break;
			case Picture.DIB:
				mediaType = "image/bmp";
				break;
			}

			handleEmbeddedResource(
					TikaInputStream.get(pic.getData()), null, null,
					mediaType, xhtml, true);
			
		}

		xhtml.endElement("div");
	}

	private void handleSlideEmbeddedResources(Slide slide, XHTMLContentHandler xhtml)
			throws TikaException, SAXException, IOException {
		Shape[] shapes;
		try {
			shapes = slide.getShapes();
		} catch(NullPointerException e) {
			// Sometimes HSLF hits problems
			// Please open POI bugs for any you come across!
			return;
		}

		for( Shape shape : shapes ) {
			if( shape instanceof OLEShape ) {
				OLEShape oleShape = (OLEShape)shape;
				ObjectData data = null;
				try {
					data = oleShape.getObjectData();
				} catch( NullPointerException e ) { 
					/* getObjectData throws NPE some times. */
				}

				if (data != null) {
					String objID = Integer.toString(oleShape.getObjectID());

					// Embedded Object: add a <div
					// class="embedded" id="X"/> so consumer can see where
					// in the main text each embedded document
					// occurred:
					AttributesImpl attributes = new AttributesImpl();
					attributes.addAttribute("", "class", "class", "CDATA", "embedded");
					attributes.addAttribute("", "id", "id", "CDATA", objID);
					xhtml.startElement("div", attributes);
					xhtml.endElement("div");

					TikaInputStream stream =
							TikaInputStream.get(data.getData());
					try {
						String mediaType = null;
						if ("Excel.Chart.8".equals(oleShape.getProgID())) {
							mediaType = "application/vnd.ms-excel";
						}
						handleEmbeddedResource(
								stream, objID, objID,
								mediaType, xhtml, true);
					} finally {
						stream.close();
					}
				}
			}
		}
	}
}
