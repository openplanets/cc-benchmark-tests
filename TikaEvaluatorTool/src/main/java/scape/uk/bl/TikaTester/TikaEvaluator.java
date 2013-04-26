package scape.uk.bl.TikaTester;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class TikaEvaluator {

	private ArrayList<String> filesToIdentify = new ArrayList<String>();
	private String startTime;
	private String endTime;
	
	public TikaEvaluator(ArrayList<String> filesToIdentify) {
		this.filesToIdentify = filesToIdentify;
	}
	
	public Map<String, ArrayList<String>> evaluate() {
			
		// Identify the files
		FileIdentifier fileIdentifier = new FileIdentifier(filesToIdentify);
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SS");
		startTime = dateFormat.format(new Date());
		System.out.println("Starting file identification at " + startTime);
		Map<String, ArrayList<String>> fileTypes = fileIdentifier.identifyFiles();
		Iterator it = fileTypes.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();
	        System.out.println(fileTypePairs.getKey() + " = " + fileTypePairs.getValue());
	    }
	    endTime = dateFormat.format(new Date());
	    System.out.println("Completed file identification at " + endTime);

		return fileTypes;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	
	
}
