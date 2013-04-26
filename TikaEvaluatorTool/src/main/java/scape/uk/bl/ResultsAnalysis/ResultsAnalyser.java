package scape.uk.bl.ResultsAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import scape.uk.bl.TikaTester.Constants;
import scape.uk.bl.TikaTester.HadoopConstants;
import scape.uk.bl.TikaTester.Utilities;

/**
 * @author lmarwood
 * <P>
 * ResultsAnalyser class contains methods for analysing the results returned by the 
 * Tika evaluation. It compares the results of the evaluation was groundtruths and creates
 * an analysis file.
 */
public class ResultsAnalyser {

	private MimeTypeMapper mimeTypeMapper;
	//private String mimeTypeMapperFile;
	private ArrayList<String> report = new ArrayList<String>();
	private int successCount = 0;
	private int failCount = 0;
	private int totalCount = 0;
	private int totalTimeTaken = 0;
	private int noMimeTypeCount = 0;
	private int errorCount = 0;
	private String reportFile = "";
	

	public ResultsAnalyser(MimeTypeMapper mimeTypeMapper) {
		 this.mimeTypeMapper = mimeTypeMapper;
		 //report.add("File Path,File Name,Tika Mime Type, GroundTruth Mime Types, Result" + Constants.NEW_LINE);
	}
	
	//public ResultsAnalyser(String mimeTypeMapperFile) throws IOException, FileNotFoundException {
	//	this.mimeTypeMapperFile = mimeTypeMapperFile;
	//	mimeTypeMapper = new MimeTypeMapper(this.mimeTypeMapperFile);
	//	report.add("File Path,File Name,Tika Mime Type, GroundTruth Mime Types, Result" + Constants.NEW_LINE);
	//}
	/**
	 * Analyse the results of running the Tika identification tool 
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public ArrayList<String> analyseResults(String tikaResultsFile) throws IOException, FileNotFoundException {
		
		report.add("File Path,File Name,Tika Mime Type,GroundTruth Mime Types,Time Taken,Result" + Constants.NEW_LINE);
		System.out.println("Reading Tika results file " + tikaResultsFile);
		BufferedReader reader = null;
		int lineCounter = 0;
		boolean appendToReport = false;
		try {
		    reader = new BufferedReader(new FileReader(tikaResultsFile));
		    String text = null;
		    while ((text = reader.readLine()) != null) {
		    	// Ignore header and summary lines
		    	lineCounter++;
		    	if (lineCounter > 1 && !text.startsWith("Summary") && text.length() > 0) {
			    	report.add(analyseResult(text.trim()));  
			    	if (lineCounter%50000 == 0) {
			    		Utilities.createReportFile(reportFile, report, appendToReport);
			    		appendToReport = true;
			    		report.clear();
			    	}
		    	}
		    }
		} finally {
		    if (reader != null) reader.close(); 
		}
		report.add("Summary:");
		report.add("Files processed=" + this.getTotalCount()); 
		report.add("Successfully identified=" + this.getSuccessCount());
		report.add("Failed to identify=" + this.getFailCount()); 
		report.add("No mime type=" + this.getNoMimeTypeCount()); 
		report.add("Errors=" + this.getErrorCount());
		report.add("Total time taken=" + this.totalTimeTaken);
		Utilities.createReportFile(reportFile, report, appendToReport);
		return report;
	}
	
	/**
	 * 
	 * @param result
	 * @return
	 */
	public String analyseResult(String result) {
		
		String outcome = "FAIL";
		//System.out.println("Analysing " + result);
		String[] resultElements = result.split(",");
		String fileName = "";
		String tikaMimeType = "";
		String filePath = "";
		String tikaTimeTaken = "";
		totalCount++;
		ArrayList<String> mimeTypes = new ArrayList<String>();
		if (resultElements.length == 3) {
			filePath = Utilities.removeQuotes(resultElements[0]);
			fileName = Utilities.getFileName(filePath);
			tikaMimeType = Utilities.removeQuotes(resultElements[1]);
			tikaTimeTaken = Utilities.removeQuotes(resultElements[2]);
			System.out.println(filePath + " " + fileName + " " + tikaMimeType);
			String fileId = fileName.substring(0, fileName.indexOf("."));
			mimeTypes = mimeTypeMapper.getMimeTypes(fileId);
			System.out.println("Mime types = " + mimeTypes);
			if (mimeTypes != null && mimeTypes.size() > 0)	{
				for (String mt: mimeTypes) {
					System.out.println("Comparing [" + tikaMimeType + "] with [" + mt + "]");
					if (tikaMimeType.equals(mt)) {
						outcome = "OK";
						successCount++;
						break;
					}
				}
			} else {
				outcome = "FAIL - No mimetype for " + fileName + " [" + tikaMimeType + "]";
				noMimeTypeCount++;
			}
			// Time taken
			if (tikaTimeTaken != null) {
				try {
					int timeTaken = new Integer(tikaTimeTaken);
					totalTimeTaken+=timeTaken;
				} catch (NumberFormatException nfe) {
					
				}
				
			}
		} else {
			System.out.println("Unable to parse result line [" + result + "]");
			outcome = "ERROR PARSING RESULT LINE [" + result + "]";
			errorCount++;
		}
		
		if ("FAIL".equals(outcome)) failCount++;
		
		String resultOfAnalysis = generateReportLine(filePath, fileName, tikaMimeType, mimeTypes, tikaTimeTaken, outcome);
		return resultOfAnalysis;	
	}
	
	public void printReport(String reportFile) throws IOException {
		
		Utilities.createReportFile(reportFile, report);

	}
	
	private String generateReportLine(String filePath, String fileName, String tikaMimeType, ArrayList<String> mimeTypes, String timeTaken, String outcome) {
		StringBuffer reportLine = new StringBuffer("");
		reportLine.append(filePath);
		reportLine.append(",");
		reportLine.append(fileName);
		reportLine.append(",");
		reportLine.append(tikaMimeType);
		reportLine.append(",");
		StringBuffer mimeTypesList = new StringBuffer("");
		if (mimeTypes != null) {
			for (String mimeType: mimeTypes) {
				mimeTypesList.append(mimeType);
				mimeTypesList.append("; ");
			}
		}
		reportLine.append(mimeTypesList);
		reportLine.append(",");
		reportLine.append(timeTaken);
		reportLine.append(",");
		reportLine.append(outcome);
		//reportLine.append(Constants.NEW_LINE);

		return reportLine.toString();
	}
	
	public int getSuccessCount() {
		return successCount;
	}

	public int getFailCount() {
		return failCount;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public int getNoMimeTypeCount() {
		return noMimeTypeCount;
	}

	public int getErrorCount() {
		return errorCount;
	}

	public MimeTypeMapper getMimeTypeMapper() {
		return mimeTypeMapper;
	}

	public void setMimeTypeMapper(MimeTypeMapper mimeTypeMapper) {
		this.mimeTypeMapper = mimeTypeMapper;
	}
	
	public String getReportFile() {
		return reportFile;
	}

	public void setReportFile(String reportFile) {
		this.reportFile = reportFile;
	}

	public int getTotalTimeTaken() {
		return totalTimeTaken;
	}

	public void setTotalTimeTaken(int totalTimeTaken) {
		this.totalTimeTaken = totalTimeTaken;
	}
	
	

}
