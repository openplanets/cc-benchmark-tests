package scape.uk.bl.TikaTester;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.Tika;

public class FileIdentifier {

	private ArrayList<String> inputFileList = new ArrayList<String>() ;

    public FileIdentifier(ArrayList<String> inputFileList) {     
    	this.inputFileList = inputFileList;   
    }

    public Map<String, ArrayList<String>> identifyFiles() {
    
    	Map<String, ArrayList<String>> fileTypes = identifyFiles(inputFileList);
    	return fileTypes;
    }
    	
    public Map<String, ArrayList<String>> identifyFiles(ArrayList<String> inputFileList) {
    	
    	System.out.println("  Identifying list of input files :-");

    	Map<String, ArrayList<String>> fileTypes = new HashMap<String, ArrayList<String>>();
    	String filetype = "Unknown";
		long startTime=0, endTime=0; 
    	Tika tika = new Tika();
    	for (String filename: inputFileList) {
			File fileToIdentify = new File(filename);
			filetype = "Unknown";
			try {
				startTime = new Date().getTime();
				filetype = tika.detect(fileToIdentify);
				endTime = new Date().getTime();
				System.out.println("Identified file " + fileToIdentify + " = " + filetype);
			} catch ( IOException ioe ) {
				System.out.println("IOException: Failed to process " + fileToIdentify.getName() + " - " + ioe.getMessage());
			} catch ( Exception e ) {
				System.out.println("Exception: Failed to process " + fileToIdentify.getName() + " - " + 
						e.getClass() + ":" + e.getMessage());
			} finally {
				ArrayList<String> fileDetails = new ArrayList<String>();
				fileDetails.add(filetype);
				fileDetails.add("" + (endTime-startTime));
				fileTypes.put(filename, fileDetails);
			}
		}
		return fileTypes;
    }
	
	
}
