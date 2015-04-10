package com.dt.xfin.data;

import java.util.ArrayList;
import java.util.List;

public class PowerPointSlide {
	private int slideNumber;
	public int getSlideNumber() {
		return slideNumber;
	}
	public void setSlideNumber(int slideNumber) {
		this.slideNumber = slideNumber;
	}
	public String getHeader() {
		return header;
	}
	public void setHeader(String header) {
		this.header = header;
	}
	public String getFooter() {
		return footer;
	}
	public void setFooter(String footer) {
		this.footer = footer;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	
	
	private String header;
	private String footer;
	private String content;
	
	private List<String> comments=new ArrayList<String>();
	public List<String> getComments() {
		return comments;
	}
	public void setComments(List<String> comments) {
		this.comments = comments;
	}
	public List<String> getNotes() {
		return notes;
	}
	public void setNotes(List<String> notes) {
		this.notes = notes;
	}


	private List<String>  notes=new ArrayList<String>();;
	
	private  List<String>  image_links=new ArrayList<String>();;
	public  List<String> getImage_links() {
		return image_links;
	}
	public void setImage_links( List<String>  image_links) {
		this.image_links = image_links;
	}
	

}
