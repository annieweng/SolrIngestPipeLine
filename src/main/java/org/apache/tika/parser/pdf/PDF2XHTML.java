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
package org.apache.tika.parser.pdf;

import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObjectImage;
import org.apache.pdfbox.pdmodel.interactive.action.type.PDAction;
import org.apache.pdfbox.pdmodel.interactive.action.type.PDActionURI;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationLink;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationMarkup;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDDocumentOutline;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineItem;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineNode;
import org.apache.pdfbox.util.Matrix;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.pdfbox.util.TextPosition;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOExceptionWithCause;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Utility class that overrides the {@link PDFTextStripper} functionality
 * to produce a semi-structured XHTML SAX events instead of a plain text
 * stream.
 */
class PDF2XHTML extends PDFTextStripper {

    // TODO: remove once PDFBOX-1130 is fixed:
    private boolean inParagraph = false;
  

    /**
     * Converts the given PDF document (and related metadata) to a stream
     * of XHTML SAX events sent to the given content handler.
     *
     * @param document PDF document
     * @param handler SAX content handler
     * @param metadata PDF metadata
     * @throws SAXException if the content handler fails to process SAX events
     * @throws TikaException if the PDF document can not be processed
     */
    public static void process(
            PDDocument document, ContentHandler handler, Metadata metadata,
            boolean extractAnnotationText, boolean enableAutoSpace,
            boolean suppressDuplicateOverlappingText, boolean sortByPosition)
            throws SAXException, TikaException {
        try {
            // Extract text using a dummy Writer as we override the
            // key methods to output to the given content
            // handler.
            PDF2XHTML pdf2XHTML = new PDF2XHTML(handler, metadata,
                                                extractAnnotationText, enableAutoSpace,
                                                suppressDuplicateOverlappingText, sortByPosition);
            pdf2XHTML.writeText(document, new Writer() {
                @Override
                public void write(char[] cbuf, int off, int len) {
                }
                @Override
                public void flush() {
                }
                @Override
                public void close() {
                }
            });

        } catch (IOException e) {
            if (e.getCause() instanceof SAXException) {
                throw (SAXException) e.getCause();
            } else {
                throw new TikaException("Unable to extract PDF content", e);
            }
        }
    }

    private final XHTMLContentHandler handler;
    private final boolean extractAnnotationText;

    private PDF2XHTML(ContentHandler handler, Metadata metadata,
                      boolean extractAnnotationText, boolean enableAutoSpace,
                      boolean suppressDuplicateOverlappingText, boolean sortByPosition)
            throws IOException {
        this.handler = new XHTMLContentHandler(handler, metadata);
        this.extractAnnotationText = extractAnnotationText;
        setForceParsing(true);
        setSortByPosition(sortByPosition);
        if (enableAutoSpace) {
            setWordSeparator(" ");
        } else {
            setWordSeparator("");
        }
        // TODO: maybe expose setting these too:
        //setAverageCharTolerance(1.0f);
        //setSpacingTolerance(1.0f);
        setSuppressDuplicateOverlappingText(suppressDuplicateOverlappingText);
    }

    void extractBookmarkText() throws SAXException {
        PDDocumentOutline outline = document.getDocumentCatalog().getDocumentOutline();
        if (outline != null) {
            extractBookmarkText(outline);
        }
    }

    void extractBookmarkText(PDOutlineNode bookmark) throws SAXException {
        PDOutlineItem current = bookmark.getFirstChild();
        if (current != null) {
            handler.startElement("ul");
            while (current != null) {
                handler.startElement("li");
                handler.characters(current.getTitle());
                handler.endElement("li");
                // Recurse:
                extractBookmarkText(current);
                current = current.getNextSibling();
            }
            handler.endElement("ul");
        }
    }

    @Override
    protected void startDocument(PDDocument pdf) throws IOException {
        try {
            handler.startDocument();
        } catch (SAXException e) {
            throw new IOExceptionWithCause("Unable to start a document", e);
        }
    }

    @Override
    protected void endDocument(PDDocument pdf) throws IOException {
        try {
            // Extract text for any bookmarks:
            extractBookmarkText();
            handler.endDocument();
        } catch (SAXException e) {
            throw new IOExceptionWithCause("Unable to end a document", e);
        }
    }

    @Override
    protected void startPage(PDPage page) throws IOException {
    	
        try {
        	handler.startElement("hr");
            handler.startElement("div", "class", "page");
           
       
        if(super.getImageSources().size()>0)
        {
        	
        	for(String src:getImageSources())
        	{

		 AttributesImpl attributes = new AttributesImpl();
	        attributes.addAttribute("", "src", "src", "CDATA",src);
	      //  attributes.addAttribute("", "width", "width", "CDATA", "90%");
	  
	
	        handler.startElement("img", attributes);
	        handler.endElement("img");
        	}
        	getImageSources().clear();
        	
        }
        } catch (SAXException e) {
            throw new IOExceptionWithCause("Unable to start a page", e);
        }
        writeParagraphStart();
    }
    

    @Override
    protected void endPage(PDPage page) throws IOException {

        try {
        	/*
        	 Map xobjects = getResources().getXObjects();
             PDXObject xobject = (PDXObject)xobjects.get( objectName.getName() );
             if( xobject instanceof PDXObjectImage )
             {
                 try
                 {
                     PDXObjectImage image = (PDXObjectImage)xobject;
                     PDPage page = getCurrentPage();
                     Matrix ctm = getGraphicsState().getCurrentTransformationMatrix();
                     double rotationInRadians =(page.findRotation() * Math.PI)/180;

                     
        	   Matrix ctm = getGraphicsState().getCurrentTransformationMatrix();
               double rotationInRadians =(page.findRotation() * Math.PI)/180;


               AffineTransform rotation = new AffineTransform();
               rotation.setToRotation( rotationInRadians );
               AffineTransform rotationInverse = rotation.createInverse();
               Matrix rotationInverseMatrix = new Matrix();
               rotationInverseMatrix.setFromAffineTransform( rotationInverse );
               Matrix rotationMatrix = new Matrix();
               rotationMatrix.setFromAffineTransform( rotation );

               Matrix unrotatedCTM = ctm.multiply( rotationInverseMatrix );
               float xScale = unrotatedCTM.getXScale();
               float yScale = unrotatedCTM.getYScale();

               System.out.println( "Found image[" + objectName.getName() + "] " +
                       "at " + unrotatedCTM.getXPosition() + "," + unrotatedCTM.getYPosition() +
                       " size=" + (xScale/100f*image.getWidth()) + "," + (yScale/100f*image.getHeight() ));
               
               */
    
            writeParagraphEnd();
            // TODO: remove once PDFBOX-1143 is fixed:
            if (extractAnnotationText) {
                for(Object o : page.getAnnotations()) {
                    if( o instanceof PDAnnotationLink ) {
                        PDAnnotationLink annotationlink = (PDAnnotationLink) o;
                        if (annotationlink.getAction()  != null) {
                            PDAction action = annotationlink.getAction();
                            if( action instanceof PDActionURI ) {
                                PDActionURI uri = (PDActionURI) action;
                                String link = uri.getURI();
                                if (link != null) {
                                    handler.startElement("div", "class", "annotation");
                                    handler.startElement("a", "href", link);
                                    handler.endElement("a");
                                    handler.endElement("div");
                                }
                             }
                        }
                    }

                    if (o instanceof PDAnnotationMarkup) {
                        PDAnnotationMarkup annot = (PDAnnotationMarkup) o;
                        String title = annot.getTitlePopup();
                        String subject = annot.getSubject();
                        String contents = annot.getContents();
                        // TODO: maybe also annot.getRichContents()?
                        if (title != null || subject != null || contents != null) {
                            handler.startElement("div", "class", "annotation");

                            if (title != null) {
                                handler.startElement("div", "class", "annotationTitle");
                                handler.characters(title);
                                handler.endElement("div");
                            }

                            if (subject != null) {
                                handler.startElement("div", "class", "annotationSubject");
                                handler.characters(subject);
                                handler.endElement("div");
                            }

                            if (contents != null) {
                                handler.startElement("div", "class", "annotationContents");
                                handler.characters(contents);
                                handler.endElement("div");
                            }

                            handler.endElement("div");
                        }
                    }
                }
            }
            handler.endElement("div");
            handler.endElement("hr");
        } catch (SAXException e) {
            throw new IOExceptionWithCause("Unable to end a page", e);
        }
    }

    @Override
    protected void writeParagraphStart() throws IOException {
        // TODO: remove once PDFBOX-1130 is fixed
        if (inParagraph) {
            // Close last paragraph
            writeParagraphEnd();
        }
        else
        {
        	
        }
        assert !inParagraph;
        inParagraph = true;
        try {
            handler.startElement("p");
           
           
        } catch (SAXException e) {
            throw new IOExceptionWithCause("Unable to start a paragraph", e);
        }
    }

    @Override
    protected void writeParagraphEnd() throws IOException {
        // TODO: remove once PDFBOX-1130 is fixed
        if (!inParagraph) {
            writeParagraphStart();
        }
        assert inParagraph;
        inParagraph = false;
        try {
            handler.endElement("p");
        } catch (SAXException e) {
            throw new IOExceptionWithCause("Unable to end a paragraph", e);
        }
    }

    @Override
    protected void writeString(String text) throws IOException {
        try {
            handler.characters(text);
        } catch (SAXException e) {
            throw new IOExceptionWithCause(
                    "Unable to write a string: " + text, e);
        }
    }

    @Override
    protected void writeCharacters(TextPosition text) throws IOException {
        try {
            handler.characters(text.getCharacter());
        } catch (SAXException e) {
            throw new IOExceptionWithCause(
                    "Unable to write a character: " + text.getCharacter(), e);
        }
    }

    @Override
    protected void writeWordSeparator() throws IOException {
        try {
            handler.characters(getWordSeparator());
        } catch (SAXException e) {
            throw new IOExceptionWithCause(
                    "Unable to write a space character", e);
        }
    }

    @Override
    protected void writeLineSeparator() throws IOException {
        try {
            handler.newline();
        } catch (SAXException e) {
            throw new IOExceptionWithCause(
                    "Unable to write a newline character", e);
        }
    }
    
    /**
     * This will process the contents of a page.
     *
     * @param page The page to process.
     * @param content The contents of the page.
     *
     * @throws IOException If there is an error processing the page.
     */
    
    /*
    @Override
    protected void processPage( PDPage page, COSStream content ) throws IOException
    { super.setAddMoreFormatting(true);
    	super.processPage(page, content);
    	
    	 if(page.getResources().getImages().)
        if( currentPageNo >= startPage && currentPageNo <= endPage &&
                (startBookmarkPageNumber == -1 || currentPageNo >= startBookmarkPageNumber ) &&
                (endBookmarkPageNumber == -1 || currentPageNo <= endBookmarkPageNumber ))
        {
            startPage( page );
            pageArticles = page.getThreadBeads();
            int numberOfArticleSections = 1 + pageArticles.size() * 2;
            if( !shouldSeparateByBeads )
            {
                numberOfArticleSections = 1;
            }
            int originalSize = charactersByArticle.size();
            charactersByArticle.setSize( numberOfArticleSections );
            for( int i=0; i<numberOfArticleSections; i++ )
            {
                if( numberOfArticleSections < originalSize )
                {
                    ((List<TextPosition>)charactersByArticle.get( i )).clear();
                }
                else
                {
                    charactersByArticle.set( i, new ArrayList<TextPosition>() );
                }
            }

            characterListMapping.clear();
            processStream( page, page.findResources(), content );
            writePage();
            endPage( page );
        }

    }
    
    */
}
