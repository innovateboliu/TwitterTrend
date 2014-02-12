package com.bo;

public class HashTag {
	private String content;
	
	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public HashTag(String content) {
		this.content = content;
	}
	
	@Override 
	public int hashCode() {
		return content.toLowerCase().hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		return content.toLowerCase().equals(((HashTag)o).content.toLowerCase());
	}
}
