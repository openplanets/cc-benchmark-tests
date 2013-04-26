package scape.uk.bl.TikaTester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import scape.uk.bl.ResultsAnalysis.MimeTypeMapper;
import scape.uk.bl.ResultsAnalysis.ResultsAnalyser;

public class TikaEvaluatorMain {

	/**
	 * The TikaEvaluatorMain class holds the main() method for running the Tika evaluator
	 * @param args - One or more file and/or directory names. These file name(s) will be passed  
	 * 				  to Tika for identification
	 */
	public static void main(String[] args) throws IOException {
		
		/**
		 * Validate number of arguments
		 */
		//if (args.length < 1) {
		//	System.out.println("Usage : A file name, directory name or list of files/directories must be supplied");
		//	System.exit(1);
		//}
		//ArrayList<String> inputFileList = new ArrayList<String>();
		//for (int i=0; i<args.length; i++) {
		//	inputFileList.add(args[i]);
		//}
		
		/**
		 * Generate the list of files to identify
		 */
		//FileLister fileLister = new FileLister(inputFileList);
		//ArrayList<String> filesToIdentify = fileLister.getListOfFiles();
		//for (String filename : filesToIdentify) {
		//	System.out.println("File " + filename);
		//}
 
		/**
		 * Run the Tika evaluator against the list of files to identify
		 */
		//TikaEvaluator evaluator = new TikaEvaluator(filesToIdentify);
		//Map<String, ArrayList<String>> fileTypes = evaluator.evaluate();
		//System.out.println("Tika evaluator started at " + evaluator.getStartTime() + " and finished at " + evaluator.getEndTime());
		/**
		 * Write the results of the Tika identification to a results file
		 */
		//Utilities.createResultsFile(Constants.FILE_TYPES_FILE, fileTypes, evaluator.getStartTime(), evaluator.getEndTime());
		//Utilities.createResultsFile(Constants.FILE_TYPES_FILE, fileTypes);		
		/**
		 * Create the mapper object - this maps groundtruth filenames, descriptions and mimetypes
		 */
		//MimeTypeMapper mimeTypeMapper = new MimeTypeMapper(Constants.GROUNDTRUTHS_FILE, Constants.MIME_TYPES_FILE);
		//mimeTypeMapper.generateMimeTypeMapper();
		/**
		 * Write the mapper object to a file
		 */
		//Utilities.createMimeTypeMapperFile(Constants.MIME_MAPPER_FILE, mimeTypeMapper.getGroundTruthsMap());
		//System.out.println("Created " + Constants.MIME_MAPPER_FILE);
		
		/**
		 * Create the mapper object - pass in the mime mapper file
		 */
		MimeTypeMapper mimeTypeMapper = new MimeTypeMapper(Constants.MIME_MAPPER_FILE);
		mimeTypeMapper.generateMimeTypeMapper();
		
		/**
		 *  Analyse the results - compare file types returned by Tika with the goundtruths
		 */
		ResultsAnalyser resultsAnalyser = new ResultsAnalyser(mimeTypeMapper);
		ArrayList<String> report = resultsAnalyser.analyseResults(Constants.FILE_TYPES_FILE);
		report.add("Summary:" + Constants.NEW_LINE);
		report.add("Files processed=" + resultsAnalyser.getTotalCount() + "," + 
				"Successfully identified=" + resultsAnalyser.getSuccessCount() + "," +
				"Failed to identify=" + resultsAnalyser.getFailCount() + "," + 
				"No mime type=" + resultsAnalyser.getNoMimeTypeCount() + "," + 
				"Errors=" + resultsAnalyser.getErrorCount() + Constants.NEW_LINE);
		Utilities.createReportFile(Constants.REPORT_FILE, report);
		
		System.out.println("Completed analysing results");
		

		// DEBUGGING
/*
		MimeTypeMapping mimeTypeMapper = new MimeTypeMapping(Constants.GROUNDTRUTHS_FILE, Constants.MIME_TYPES_FILE);
		Map<String, GroundTruth> groundtruthMap = mimeTypeMapper.getGroundTruths();
	
		Iterator<Entry<String, GroundTruth>> it = groundtruthMap.entrySet().iterator();
	    System.out.println("GROUNDTRUTHS");
		while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();
	        System.out.println("  " + fileTypePairs.getKey() + " = " + 
	            ((GroundTruth)fileTypePairs.getValue()).getFilename() + " " + 
	            ((GroundTruth)fileTypePairs.getValue()).getDescription() + " " +	
	            ((GroundTruth)fileTypePairs.getValue()).getMimeType());
	    }
	    */
	}
}
