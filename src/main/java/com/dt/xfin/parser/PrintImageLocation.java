package com.dt.xfin.parser;




import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.exceptions.InvalidPasswordException;
import org.apache.pdfbox.exceptions.WrappedIOException;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObjectImage;
import org.apache.pdfbox.util.Matrix;
import org.apache.pdfbox.util.PDFOperator;
import org.apache.pdfbox.util.PDFStreamEngine;
import org.apache.pdfbox.util.ResourceLoader;

import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.io.IOException;

import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * This is an example on how to get the x/y coordinates of image locations.
 *
 * Usage: java org.pdfbox.examples.util.PrintImageLocations &lt;input-pdf&gt;
 *
 * @author <a href="mailto:ben@benlitchfield.com">Ben Litchfield</a>
 * @version $Revision: 1.5 $
 */
public class PrintImageLocation extends PDFStreamEngine
{
    /**
     * Default constructor.
     *
     * @throws IOException If there is an error loading text stripper properties.
     */
    public PrintImageLocation() throws IOException
    {
        super( ResourceLoader.loadProperties( "PDFTextStripper.properties", true ) );
    }

    /**
     * This will print the documents data.
     *
     * @param args The command line arguments.
     *
     * @throws Exception If there is an error parsing the document.
     */
    public static void main( String[] args ) throws Exception
    {
        if( args.length != 1 )
        {
            usage();
        }
        else
        {
            PDDocument document = null;
            try
            {
                document = PDDocument.load( args[0] );
                if( document.isEncrypted() )
                {
                    try
                    {
                        document.decrypt( "" );
                    }
                    catch( InvalidPasswordException e )
                    {
                        System.err.println( "Error: Document is encrypted with a password." );
                        System.exit( 1 );
                    }
                }
                PrintImageLocation printer = new PrintImageLocation();
                List allPages = document.getDocumentCatalog().getAllPages();
                for( int i=0; i<allPages.size(); i++ )
                {
                    PDPage page = (PDPage)allPages.get( i );
                   Logger.getLogger(PrintImageLocation.class).debug("Processing page: " + i );
                    printer.processStream( page, page.findResources(), page.getContents().getStream() );
                }
            }
            finally
            {
                if( document != null )
                {
                    document.close();
                }
            }
        }
    }

    /**
     * This is used to handle an operation.
     *
     * @param operator The operation to perform.
     * @param arguments The list of arguments.
     *
     * @throws IOException If there is an error processing the operation.
     */
    protected void processOperator( PDFOperator operator, List arguments ) throws IOException
    {
        String operation = operator.getOperation();
        if( operation.equals( "Do" ) )
        {
            COSName objectName = (COSName)arguments.get( 0 );
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

                    Logger.getLogger(PrintImageLocation.class).debug( "Found image[" + objectName.getName() + "] " +
                            "at " + unrotatedCTM.getXPosition() + "," + unrotatedCTM.getYPosition() +
                            " size=" + (xScale/100f*image.getWidth()) + "," + (yScale/100f*image.getHeight() ));
                }
                catch( NoninvertibleTransformException e )
                {
                    throw new WrappedIOException( e );
                }
            }
        }
        else
        {
            super.processOperator( operator, arguments );
        }
    }

    /**
     * This will print the usage for this document.
     */
    private static void usage()
    {
        System.err.println( "Usage: java org.pdfbox.examples.pdmodel.PrintImageLocations <input-pdf>" );
    }

}
