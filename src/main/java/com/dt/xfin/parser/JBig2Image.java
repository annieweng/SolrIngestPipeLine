package com.dt.xfin.parser;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jpedal.jbig2.JBIG2Decoder;

import com.itextpdf.awt.geom.Rectangle2D;
import com.itextpdf.text.Document;
import com.itextpdf.text.Image;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.parser.ImageRenderInfo;
import com.itextpdf.text.pdf.parser.Matrix;
import com.itextpdf.text.pdf.parser.PdfImageObject;
import com.itextpdf.text.pdf.parser.PdfReaderContentParser;
import com.itextpdf.text.pdf.parser.RenderListener;
import com.itextpdf.text.pdf.parser.TextRenderInfo;

public class JBig2Image {
	private String filepath;
	private static int imageIndex=0;
	  /** Log instance. */
    private static final Log LOG = LogFactory.getLog(JBig2Image.class);
	private String pdfFile="/home/aweng/data/TITAN/test/test.pdf";

	protected static Map<Integer, List<String>> imageSourceMaps=new HashMap<Integer, List<String>>();

	protected static Map<Integer, byte[]> JBIG2globalsMap=new HashMap<Integer, byte[]>();
	public static Map<Integer, List<String>> getImageSourceMaps() {
		return imageSourceMaps;
	}
	private static JBIG2Decoder decoder=null;

	

	//this class handle JBIG2 encoded images in pdf file. 
	//apache PDFbox doesn't handle JBIG2 encoded images. use itext and levigo

	public JBig2Image(String pPdfFile, String outputFolder,  Map<Integer, byte[]> jBIG2_Global_map) throws IOException {
		
		
		imageSourceMaps.clear();
		pdfFile=pPdfFile;
		JBIG2globalsMap=jBIG2_Global_map;
		this.filepath = pdfFile;
		if(pdfFile.indexOf(File.separatorChar)>=0)
		{
			filepath=pdfFile.substring(0, pdfFile.lastIndexOf(File.separatorChar));
		}
		this.imageIndex = 0;


		if(outputFolder==null)
		{
			outputFolder=pdfFile.substring(0, pdfFile.toLowerCase().indexOf(".pdf"));
		}

		//extract images from pdf
		extractImgFromPdf(outputFolder);
		
		// createPDF();
	}


	private void extractImgFromPdf(String output) {
		try {
			/////////// Extract all Images from pdf /////////////////////////

			PdfReader reader = new PdfReader(pdfFile);

			PdfReaderContentParser parser = new PdfReaderContentParser(reader);
			if(output==null)
			{
				output=pdfFile.substring(0, pdfFile.toLowerCase().indexOf(".pdf"));
			}
			//go ahead make a directory with same pdf name
			File outfile=new File(output);
			outfile.mkdir();

			MyImageRenderListener listener = 
					new MyImageRenderListener(output+File.separator);
			for (int i = 1; i <= reader.getNumberOfPages(); i++) {
				listener.setPageNumber(i);
				parser.processContent(i, listener);

			}
		} catch (IOException ex) {
			System.out.println(ex);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}




	public BufferedImage readImage(File file) throws IOException {
		// Find a JPEG reader which supports reading Rasters.
		Iterator<ImageReader> readers = ImageIO.getImageReadersBySuffix("jbig2") ;

		ImageReader reader = null;
		while (readers.hasNext()) {
			reader = (ImageReader) readers.next();
			if (reader.canReadRaster()) {
				break;
			}
		}

		// Set the input.
		ImageInputStream input = ImageIO.createImageInputStream(file);
		reader.setInput(input);

		// Create the image.
		BufferedImage image = reader.read(0);
		return image;
	}





	public static  String convertJBig2ToPng(File file) {
		String outputFileName=null;
	
		try {
			///////// Read jbig2 image ////////////////////////////////////////

			//inputStream = new FileInputStream(file);
			
			decoder = new JBIG2Decoder();
			try {
				decoder.decodeJBIG2(file);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 


			  BufferedImage bi = decoder.getPageAsBufferedImage(0);

			
			 

			outputFileName=file.getCanonicalPath().replace(".jbig2", ".png");
			//  BufferedImage bufferedImage= readImage(file);
			////////// jbig2 to jpeg ///////////////////////////////////////////
			ImageIO.write(bi, "png", new File(outputFileName));

		} catch (IOException ex) {
			System.out.println(ex);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}

		
		return outputFileName;
	}

	public void createPDF() {
		Document document = new Document();
		try {
			PdfWriter.getInstance(document,
					new FileOutputStream(filepath+"output.pdf"));
			document.open();
			PdfPTable table = new PdfPTable(1); //1 column.    
			Image image = Image.getInstance(filepath);
			image.scaleToFit(800f, 600f);
			image.scaleAbsolute(800f, 600f);   // Give the size of image you want to print on pdf
			PdfPCell nestedImgCell = new PdfPCell(image);
			table.addCell(nestedImgCell);
			document.add(table);
			document.close();
			System.out.println(
					"======== PDF Created Successfully =========");
		} catch (Exception e) {
			System.out.println(e);
		}
	}



	 
}



class MyImageRenderListener implements RenderListener {
	private Rectangle2D.Float textRectangle = null;
	/**
	 * The new document to which we've added a border rectangle.
	 */
	protected String path = "";
	String content="";

	private int pageNumber;

	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;

		if(pageNumber>1)
		{
			//	ouputPdfPage();
		}

	}

	private void ouputPdfPage()
	{
		FileOutputStream os;
		try {
			String  filename = String.format(path, (pageNumber-1), "html");
			os = new FileOutputStream(filename);

			content="<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+
					"<html xmlns=\"http://www.w3.org/1999/xhtml\"><head><title>"
					+path.substring(0, path.lastIndexOf(File.separator)).substring(path.lastIndexOf(File.separator))+" Page "+(pageNumber-1)+"</title></head><body>"+content;
			content+="</body></html>";
			os.write(content.getBytes());
			os.flush();
			os.close();
		} catch (FileNotFoundException e) {  /** Log instance. */
		 
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(content);
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		content="";
	}


	/**
	 * Creates a RenderListener that will look for images.
	 */
	public MyImageRenderListener(String path) {
		this.path = path;
	}

	/**
	 * @see com.itextpdf.text.pdf.parser.RenderListener#beginTextBlock()
	 */
	public void beginTextBlock() {
		content+="<p>";
	}

	/**
	 * @see com.itextpdf.text.pdf.parser.RenderListener#endTextBlock()
	 */
	public void endTextBlock() {
		content+="</p>";

	}

	public static BufferedImage verticalflip(BufferedImage img) {  
		int w = img.getWidth();  
		int h = img.getHeight();  
		BufferedImage  dimg = new BufferedImage(w, h, img.getColorModel().getTransparency());  
		Graphics2D g = dimg.createGraphics();  
		g.drawImage(img, 0, 0, w, h, 0, h, w, 0, null);  
		g.dispose();  
		return dimg;  
	}  



	
	/**
	 * @see com.itextpdf.text.pdf.parser.RenderListener#renderImage(
	 * com.itextpdf.text.pdf.parser.ImageRenderInfo)
	 */
	public void renderImage(ImageRenderInfo renderInfo) {
		try {
			String filename;



			FileOutputStream os;
			renderInfo.getImage().getDictionary();
			Matrix matrix=renderInfo.getImageCTM();

			PdfImageObject image = renderInfo.getImage();
			/*
			for(PdfName name:image.getDictionary().getKeys())
			{
			//	System.out.println(name+": "+image.get(name).toString());

			}
			 */

			/*
    		PdfName filter = (PdfName)image.get(PdfName.FILTER);
    		if (PdfName.DCTDECODE.equals(filter)) {
    			filename = String.format(path, renderInfo.getRef().getNumber(), "jpg");
    			os = new FileOutputStream(filename);
    			os.write(image.getImageAsBytes());
    			os.flush();
    			os.close();
    		}
    		else if (PdfName.JPXDECODE.equals(filter)) {
    			filename = String.format(path, renderInfo.getRef().getNumber(), "jp2");
    			os = new FileOutputStream(filename);
    			os.write(image.getImageAsBytes());
    			os.flush();
    			os.close();
    		}
    		else if(PdfName.JBIG2DECODE.equals(filter))
    		{
    			filename = String.format(path, renderInfo.getRef().getNumber(), "jbig2");
    			os = new FileOutputStream(filename);
    			os.write(image.getImageAsBytes());
    			os.flush();
    			os.close();
    		}
    		else {
    			BufferedImage awtimage = renderInfo.getImage().getBufferedImage();
    			if (awtimage != null) {
    				filename = String.format(path, renderInfo.getRef().getNumber(), "png");
    				ImageIO.write(awtimage, "png", new FileOutputStream(filename));
    			}
    		}
			 */



			if (image == null) {
				return;
			}
			if(renderInfo.getRef()==null || image.getFileType()==null)
			{
				System.out.println("something is wrong : image type:"+image.getFileType());
			}
			else
			{
				filename = path+pageNumber+"_"+renderInfo.getRef().getNumber()+"."+image.getFileType();

				//rotate the image, the i22 is negative
				if(renderInfo.getImage().getBufferedImage()!=null )
				{
					if( matrix.get(Matrix.I22)<0)
					{
					System.out.println("Image is upside down. flip it");
					BufferedImage reversedImage=verticalflip(renderInfo.getImage().getBufferedImage());
					ImageIO.write(reversedImage,  image.getFileType(), new File(filename));
					}
					else
					{
						ImageIO.write(renderInfo.getImage().getBufferedImage(), 
								image.getFileType(), new File(filename));
					}
					
					content+="<img source=\"file://"+filename+"\" width=\"200\" />";
					System.out.println("successfully processed image type:"+image.getFileType()+" byte type"+  image.getImageBytesType()+
							" "+renderInfo.getRef().getNumber());
				}
				else
				{
					if(JBig2Image.JBIG2globalsMap.get(pageNumber)!=null)
					{
					content+="<img source=\"file://"+filename+"\" width=\"200\" />";

					os = new FileOutputStream(filename);
					/*
					os.write(renderInfo.getImage().get(PdfName.WIDTH).getBytes());
					os.write(renderInfo.getImage().get(PdfName.HEIGHT).getBytes());
					os.write(renderInfo.getImage().get(PdfName.LOCATION.X).getBytes());
					os.write(renderInfo.getImage().get(PdfName.LOCATION.Y).getBytes());
					*/
					
					os.write(JBig2Image.JBIG2globalsMap.get(pageNumber));
					os.write(image.getImageAsBytes());
					os.flush();
					os.close();
					System.out.println("successfully processed image type:"+image.getFileType()+" byte type"+  image.getImageBytesType()+
							" "+renderInfo.getRef().getNumber());
					}
					else
					{
						filename=null;
						System.out.println("something is wrong : fail to get jbig2 header info. ignore the image");
					}

				}	

				
					
			
				if(filename!=null && filename.endsWith("jbig2"))
				{
					
					filename=JBig2Image.convertJBig2ToPng(new File(filename));
				}
				if(filename!=null)
				{
					if(JBig2Image.imageSourceMaps.containsKey(pageNumber))
					{
						JBig2Image.imageSourceMaps.get(pageNumber).add(filename);
					}
					else
					{
						List<String> images=new ArrayList<String>();
						images.add(filename);
						JBig2Image.imageSourceMaps.put(pageNumber, images);
					}
				}
			}

		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}






	/**
	 * @see com.itextpdf.text.pdf.parser.RenderListener#renderText(
	 * com.itextpdf.text.pdf.parser.TextRenderInfo)
	 */
	public void renderText(TextRenderInfo renderInfo) {


		content+=renderInfo.getText();
	}
}
