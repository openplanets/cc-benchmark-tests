package scape.uk.bl.ResultsAnalysis;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import scape.uk.bl.TikaTester.Utilities;

/**
 * 
 * @author lmarwood
 * Uses contents of groundtruths CSV file and mime type decoder file to create a mapping
 * between filename, description of file type amd mime type
 */
public class MimeTypeMapper {

	private Map<String, ArrayList<String>> mimeTypesMap = new HashMap<String, ArrayList<String>>();
	private Map<String, GroundTruth> groundTruthsMap = new HashMap<String, GroundTruth>();
	private String groundTruthFileName;
	private String mimeTypeDecoderFileName;
	private String mimeTypeMapperFile;
	
	public MimeTypeMapper(String mimeTypeMapperFile) {
		this.mimeTypeMapperFile = mimeTypeMapperFile;
	}
	
	public MimeTypeMapper(String groundTruthFileName, String mimeTypeDecoderFileName) {
		this.groundTruthFileName = groundTruthFileName;
		this.mimeTypeDecoderFileName = mimeTypeDecoderFileName;
	}
	
	/**
	 * Can generate groundtruthsmap from the ground truth file and mime type decoder file
	 * or from a mime type mapper file depending on which constructor was used.
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public void generateMimeTypeMapper() throws IOException, FileNotFoundException {
		if (groundTruthFileName != null && mimeTypeDecoderFileName != null) {
			populateMimeTypes();
			createGroundtruthsMap();
		} else if (mimeTypeMapperFile != null) {
			readMapperFile();
		}
	}
	
	/**
	 * Create a map of File description objects that contain the filename, 
	 * description and mime type
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private void createGroundtruthsMap() throws IOException, FileNotFoundException {
		
		BufferedReader reader = null;
		try {
		    reader = new BufferedReader(new FileReader(groundTruthFileName));
		    String text = null;
		    int lineCounter = 0;
		    while ((text = reader.readLine()) != null) {
		    	lineCounter++;
		    	//if (lineCounter > 1000) {
		    	//	break;
		    	//}
		    	// Ignore header
		    	if (lineCounter > 1) {
		    		String[] groundTruthsList = text.split(",");
		    		if (groundTruthsList.length >= 3) {
		    			GroundTruth groundTruth = new GroundTruth();
		    			String filename = Utilities.removeQuotes(groundTruthsList[0]);
		    			groundTruth.setFilename(filename);
		    			groundTruth.setDescription(Utilities.removeQuotes(groundTruthsList[2]));
		    			ArrayList<String> mimeTypes = mimeTypesMap.get(Utilities.removeQuotes(groundTruthsList[2]));
		    			groundTruth.setMimeType(mimeTypes);
		    			groundTruthsMap.put(filename, groundTruth);
		    		}
		    		else {
		    			System.out.println("Error in groundtruths file [" + text + "]");
		    		}
		    	}
		    }
		    
		} finally {
		    if (reader != null) reader.close(); 
		}
		
		System.out.println("Obtained " + groundTruthsMap.size() + " groundtruths ");
		Iterator<Entry<String, GroundTruth>> it = groundTruthsMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();
	        System.out.println("  " + fileTypePairs.getKey() + "=" + 
	        		((GroundTruth)fileTypePairs.getValue()).getDescription() + ",  Mime types=" +
	        		((GroundTruth)fileTypePairs.getValue()).getMimeTypes());
	    }
	    System.out.println("End of groundtruths [" + groundTruthsMap.size() + "]");
	}
	
	/**
	 * Create a list of mime description/type mappings
	 * This associates mime types with the file descriptions in the ground truths file
	 * and enables the results from running Tika identification to be analysed
	 *
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private void populateMimeTypes() throws IOException, FileNotFoundException  {
		//ArrayList<String> mimeTypes = new ArrayList<String>();
		BufferedReader reader = null;
		try {
		    reader = new BufferedReader(new FileReader(mimeTypeDecoderFileName));
		    String text = null;
		    int lineCounter = 0;
		    while ((text = reader.readLine()) != null) {
		    	lineCounter++;
	    		String[] mimeTypesList = text.split(",");
	    		// If the file description already exists in then mimeTypes map then add the new 
	    		// mime type to the list of mime types associated with this file description
	    		// Otherwise add a new mime description/type mapping
	    		if (mimeTypesList.length >= 2) {
	    			ArrayList<String> mimeTypes;
	    			if (mimeTypesMap.containsKey(mimeTypesList[1])) {
	    				mimeTypes = mimeTypesMap.get(mimeTypesList[1]);
	    			} else {
	    				mimeTypes = new ArrayList<String>();
	    			}
	    			
	    			mimeTypes.add(Utilities.removeQuotes(mimeTypesList[0]));
    				mimeTypesMap.put(Utilities.removeQuotes(mimeTypesList[1]), mimeTypes);
	    		}
		    }
		} finally {
		    if (reader != null) reader.close();
		}
		
		System.out.println("Obtained " + mimeTypesMap.size() + " mime description/type mappings from " + mimeTypeDecoderFileName);
		Iterator<Entry<String, ArrayList<String>>> it = mimeTypesMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();
	        System.out.println("  " + fileTypePairs.getKey() + " = " + ((ArrayList<String>)fileTypePairs.getValue()).toString());
	    }
	    System.out.println("End of mime description/type mappings [" + mimeTypesMap.size() + "]");
	}

	private void readMapperFile() throws IOException, FileNotFoundException {
		
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(this.mimeTypeMapperFile));
			String text = null;
			int lineCounter = 0;
			while ((text = reader.readLine()) != null) {
				lineCounter++;
				String[] mapperElements = text.split(",");
				GroundTruth groundtruth = new GroundTruth();
				if (mapperElements.length >= 3) {
					groundtruth.setFilename(Utilities.removeQuotes(mapperElements[0]));
					groundtruth.setDescription(Utilities.removeQuotes(mapperElements[1]));
					String mimeTypesList=Utilities.removeQuotes(mapperElements[2]);
					if (mimeTypesList != null) {
						String[] mimeTypes = mimeTypesList.split(";");
						for (int i=0; i<mimeTypes.length; i++) {
							groundtruth.addMimeType(mimeTypes[i]);
						}
					}
				}
				String fileIdentifier = groundtruth.getFilename();
				if (groundtruth.getFilename().indexOf(".") > 0) {
					fileIdentifier = groundtruth.getFilename().substring(0, groundtruth.getFilename().indexOf("."));
				}
				groundTruthsMap.put(fileIdentifier, groundtruth);
			}
		} finally {
			if (reader != null) reader.close(); 
		}	
		
		System.out.println("Obtained " + groundTruthsMap.size() + " groundtruths ");
		Iterator<Entry<String, GroundTruth>> it = groundTruthsMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();
	        System.out.println("  " + fileTypePairs.getKey() + "=" + 
	        		((GroundTruth)fileTypePairs.getValue()).getDescription() + ",  Mime types=" +
	        		((GroundTruth)fileTypePairs.getValue()).getMimeTypes());
	    }
	    System.out.println("End of groundtruths [" + groundTruthsMap.size() + "]");
	}
	
	
	public Map<String, GroundTruth> getGroundTruthsMap() {
		return groundTruthsMap;
	}
	
	public void setGroundTruthsMap(Map<String, GroundTruth> groundTruthsMap) {
		this.groundTruthsMap = groundTruthsMap;
	}
	
	public ArrayList<String> getMimeTypes(String fileId) {
		ArrayList<String> mimeTypes = null;
		System.out.println("Getting mimetypes for " + fileId);
		GroundTruth groundTruth = groundTruthsMap.get(fileId);
		if (groundTruth != null) {
			mimeTypes = groundTruth.getMimeTypes();
			System.out.println("Found " + mimeTypes.size() + " mimetypes for " + fileId + "[" + mimeTypes + "]");
		} else {
			System.out.println("Failed to find mimetypes for " + fileId);
		}
		return mimeTypes;
	}

	//public void setGroundTruths(Map<String, GroundTruth> groundTruths) {
	//	this.groundTruths = groundTruths;
	//}
	

}
