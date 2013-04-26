package scape.uk.bl.TikaTester;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import scape.uk.bl.ResultsAnalysis.MimeTypeMapper;
import scape.uk.bl.ResultsAnalysis.ResultsAnalyser;

public class TikaResultAnalyser {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

		/**
		 * Validate number of arguments
		 */
		if (args.length < 4) {
			System.out.println("Usage : (1) Results directory, (2) Name of results file, (3) Location of mime mapper (groundtruths) file, (4) Name of the report file");
			System.exit(1);
		}
		String resultsDir = args[0];
		String resultsFile = args[1];
		String mimeMapper = args[2];
		String reportFile = args[3];
		/**
		 * Copy the output from the reduce process to the results directory
		 */
		File hadoopResultsDirectory = new File(resultsDir);
		if (!hadoopResultsDirectory.isDirectory()) {
			System.out.println("RESULTS DIRECTORY - " + hadoopResultsDirectory + " is not a directory");
			System.exit(1);
		}
		File results = new File(resultsDir + resultsFile); 
		if (!results.exists() || !results.isFile()) {
			System.out.println("RESULTS FILE - " + resultsDir + resultsFile + " does not exist or is not a file");
		}
		
		File mimeMapperFile =  new File (mimeMapper);
		if (!mimeMapperFile.exists() || !mimeMapperFile.isFile()) {
			System.out.println("MIME MAPPER FILE - " + mimeMapper + " does not exist or is not a file");
		}
		

		File[] hadoopOutputFiles = hadoopResultsDirectory.listFiles();
		int totalLines=0;
		boolean appendToResultsFile = false;
		for (int i=0; i<hadoopOutputFiles.length; i++) {
			if (!hadoopOutputFiles[i].isFile()) {
				System.out.println(hadoopOutputFiles[i].getPath() + " is not a file - ignoring");
			}
			System.out.println("Processing " + hadoopOutputFiles[i].getPath());
			Map<String, ArrayList<String>> fileTypes = new HashMap<String, ArrayList<String>>();
			int lineCounter=0;
			String hadoopOutputFileName = hadoopOutputFiles[i].getName();
			if (hadoopOutputFiles[i].length() > 0 && hadoopOutputFileName.startsWith(HadoopConstants.HADOOP_LOCAL_OUTPUT_FILE_PREFIX)) {
				System.out.println("Processing Hadoop output file " + hadoopOutputFileName);	
				BufferedReader reader = null;
				try {
				    reader = new BufferedReader(new FileReader(hadoopOutputFiles[i].getPath()));
				    String text = null;
				    while ((text = reader.readLine()) != null) {
				    	lineCounter++;
				    	String[] fileMimeTypes = text.split(",");
				    	if (fileMimeTypes.length == 3) {
							ArrayList<String> fileIdentifcationDetails = new ArrayList<String>();
				    		fileIdentifcationDetails.add(fileMimeTypes[1]);
				    		fileIdentifcationDetails.add(fileMimeTypes[2]);
				    		fileTypes.put(fileMimeTypes[0].trim(), fileIdentifcationDetails);
				    	} else {
				    		System.out.println("Line " + lineCounter + "has incorrect number of elements [" + fileMimeTypes.length + "] - " + text);
				    	}
				    }
				} finally {
				    if (reader != null) reader.close(); 
				    System.out.println("Completed processing of Hadoop output file " + hadoopOutputFileName + ", " + lineCounter + " lines");
				    totalLines+=lineCounter;
				    Utilities.createResultsFile(resultsDir + resultsFile, fileTypes, appendToResultsFile);
					fileTypes = null;
					appendToResultsFile = true;
				}
			}
		}	
		System.out.println("Completed processing of all Hadoop output files, total lines=" + totalLines + " lines");
		
		/**
		 * Create the mime type mapping
		 */
		MimeTypeMapper mimeTypeMapper = new MimeTypeMapper(mimeMapper);
		ResultsAnalyser resultsAnalyser = new ResultsAnalyser(mimeTypeMapper);
		System.out.println("Generating mime types mapper");
		mimeTypeMapper.generateMimeTypeMapper();
		resultsAnalyser.setMimeTypeMapper(mimeTypeMapper);
		System.out.println("ResultsAnalyser has " + resultsAnalyser.getMimeTypeMapper().getGroundTruthsMap().size() + " mime maps");
		
		//mimeTypeMapper.getMimeTypes("203473");
		//mimeTypeMapper.getMimeTypes("904789");
		//mimeTypeMapper.getMimeTypes("208001");
		//mimeTypeMapper.getMimeTypes("715323");
		
		/**
		 * Analyse the results
		 */
		System.out.println("Analysing results");
		resultsAnalyser.setReportFile(resultsDir + reportFile);
		ArrayList<String> report = resultsAnalyser.analyseResults(resultsDir + resultsFile);
		
		//report.add("Summary:" + HadoopConstants.NEW_LINE);
		//report.add("Files processed=" + resultsAnalyser.getTotalCount() + HadoopConstants.NEW_LINE); 
		//report.add("Successfully identified=" + resultsAnalyser.getSuccessCount() + HadoopConstants.NEW_LINE);
		//report.add("Failed to identify=" + resultsAnalyser.getFailCount() + HadoopConstants.NEW_LINE); 
		//report.add("No mime type=" + resultsAnalyser.getNoMimeTypeCount() + HadoopConstants.NEW_LINE); 
		//report.add("Errors=" + resultsAnalyser.getErrorCount() + HadoopConstants.NEW_LINE);
		//Utilities.createReportFile(resultsDir + reportFile, report, true);


	}

}
