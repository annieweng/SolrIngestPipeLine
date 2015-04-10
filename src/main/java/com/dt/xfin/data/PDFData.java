package com.dt.xfin.data;

import java.util.ArrayList;
import java.util.List;

public class PDFData {
	private int pageNumber;
	public int getPageNumber() {
		return pageNumber;
	}
	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}
	
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	

	private String content;
	
	


	
	
	private  List<String>  image_links=new ArrayList<String>();;
	public  List<String> getImage_links() {
		return image_links;
	}
	public void setImage_links( List<String>  image_links) {
		this.image_links = image_links;
	}
	

}
