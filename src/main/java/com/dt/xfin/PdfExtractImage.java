package com.dt.xfin;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;



import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDPixelMap;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObjectImage;

public class PdfExtractImage {

	public static void main(String[] args) {
		PdfExtractImage obj = new PdfExtractImage();
		    try {
		        obj.read_pdf();
		    } catch (IOException ex) {
		        System.out.println("" + ex);
		    }

		}

		 void read_pdf() throws IOException {
		    PDDocument document = null; 
		    try {
		        document = PDDocument.load("/home/aweng/data/TITAN/test/CS-74.PDF");
		    } catch (IOException ex) {
		        System.out.println("" + ex);
		    }
		    List pages = document.getDocumentCatalog().getAllPages();
		    Iterator iter = pages.iterator(); 
		    int i =1;
		    String name = null;

		    while (iter.hasNext()) {
		        PDPage page = (PDPage) iter.next();
		        PDResources resources = page.getResources();
		        Map pageImages = resources.getImages();
		        if (pageImages != null) { 
		            Iterator imageIter = pageImages.keySet().iterator();
		            while (imageIter.hasNext()) {
		                String key = (String) imageIter.next();
		                PDXObjectImage image = (PDXObjectImage) pageImages.get(key);
		               /*
		                if(image instanceof PDPixelMap)
		                {
		                	image.
		                }
		                */
		                image.write2file("/home/aweng/data/TITAN/test/" + i);
		                /*
		                File outputFile = new File( "/home/aweng/data/TITAN/test/" + i+".png");
		                ImageIO.write( image.getRGBImage(), "png", outputFile);
		                */
		                i ++;
		            }
		        }
		    }

		}
}
