package scape.uk.bl.ResultsAnalysis;

import java.util.ArrayList;

public class GroundTruth {

	private String filename;
	private String description;
	private ArrayList<String> mimeTypes = new ArrayList<String>();
	
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public void addMimeType(String mimeType) {
		this.mimeTypes.add(mimeType);
	}
	public ArrayList<String> getMimeTypes() {
		return mimeTypes;
	}
	public void setMimeType(ArrayList<String> mimeTypes) {
		this.mimeTypes = mimeTypes;
	}

}
