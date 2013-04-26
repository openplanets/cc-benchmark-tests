package scape.uk.bl.TikaTester;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tika.Tika;

import scape.uk.bl.ResultsAnalysis.MimeTypeMapper;
import scape.uk.bl.ResultsAnalysis.ResultsAnalyser;

public class TikaEvaluatorHadoopMain extends Configured implements Tool {
	
	//public static int lineCounter = 0;
	public static MimeTypeMapper mimeTypeMapper = new MimeTypeMapper(HadoopConstants.MIME_MAPPER_FILE);
	public static ResultsAnalyser resultsAnalyser = new ResultsAnalyser(mimeTypeMapper);
	
	/*
	public static class TikaIdentifierMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
    	private Tika tika = new Tika();
    	public static MimeTypeMapper mimeTypeMapper2;
    	public ResultsAnalyser resultsAnalyser;
    	
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			StringBuffer resultOfAnalysis = new StringBuffer("");
			if (TikaIdentifierMapper.mimeTypeMapper2 == null || TikaIdentifierMapper.mimeTypeMapper2.getGroundTruthsMap() == null || TikaIdentifierMapper.mimeTypeMapper2.getGroundTruthsMap().isEmpty()) {
				//TikaIdentifierMapper.mimeTypeMapper2.generateMimeTypeMapper();
				TikaIdentifierMapper.mimeTypeMapper2.setGroundTruthsMap(TikaEvaluatorHadoopMain.mimeTypeMapper.getGroundTruthsMap());
			}
			System.out.println("TikaIdentifierMapper : map() - Key=" + key.toString() + ", Value=" + value.toString());
			resultsAnalyser = new ResultsAnalyser(TikaIdentifierMapper.mimeTypeMapper2);
			ArrayList<String> inputFileList = new ArrayList<String>();
			inputFileList.add(value.toString());
				
	    	String filetype = "";		
			String filename = value.toString();
			File fileToIdentify = new File(filename);
			try {
				long startTime = new Date().getTime();
				filetype = tika.detect(fileToIdentify);
				long endTime = new Date().getTime();
				//fileTypes.put(filename, filetype);
				if (resultsAnalyser == null) {
					resultOfAnalysis.append("ResultsAnalyser is null");
				} else if (filename == null || filetype == null) {
					resultOfAnalysis.append("filename=" + filename + " filetype=" + filetype);
				} else if (mimeTypeMapper2 == null) { 
					resultOfAnalysis.append("MimeTypeMapper is null");
				} else if (mimeTypeMapper2.getGroundTruthsMap() == null) {
					resultOfAnalysis.append("MimeTypeMapper groundTruthsMap is null");
				} else if (mimeTypeMapper2.getGroundTruthsMap().isEmpty()) {
					resultOfAnalysis.append("MimeTypeMapper groundTruthsMap is empty");
				} else {
					resultOfAnalysis.append(resultsAnalyser.analyseResult(filename + "," + filetype));
					resultOfAnalysis.append("," + (endTime-startTime) + HadoopConstants.NEW_LINE);
				}
			} catch ( IOException ioe ) {
				System.out.println("IOException: Failed to process " + fileToIdentify.getName() + " - " + ioe.getMessage());
				//fileTypes.put(filename, "IOEXCEPTION " + ioe.getMessage());
				resultOfAnalysis.append(filename + "," + "IOEXCEPTION " + ioe.getMessage() + HadoopConstants.NEW_LINE);
			} catch ( Exception e ) {
				System.out.println("Exception: Failed to process " + fileToIdentify.getName() + " - " + e.getClass() + ":" + e.getMessage());
				//fileTypes.put(filename, "EXCEPTION " + e.getMessage());
				StackTraceElement[] stackTrace = e.getStackTrace();
				StringBuffer stackBuffer = new StringBuffer();
				for (int i=0; i<stackTrace.length; i++) {
					stackBuffer.append(stackTrace[i] + HadoopConstants.NEW_LINE);
				}
				resultOfAnalysis.append(filename + "," + "EXCEPTION " + e.getClass() + " " + stackBuffer + HadoopConstants.NEW_LINE);
			} 
			
			//output.collect(new Text(key.toString()), new Text(filename + "," + filetype + HadoopConstants.NEW_LINE));
			output.collect(new Text(key.toString()), new Text(resultOfAnalysis.toString()));
			resultOfAnalysis.setLength(0);
			System.out.println("TikaIdentifierMapper completed");
		}
		
	} */
	
	
	public static class TikaIdentifierMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
    	private Tika tika = new Tika();
    	
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			StringBuffer result = new StringBuffer("");
			System.out.println("TikaIdentifierMapper : map() - Key=" + key.toString() + ", Value=" + value.toString());	
	    	String filetype = "";		
			String filename = value.toString();
			File fileToIdentify = new File(filename);
			try {
				//long startTime = new Date().getTime();
				filetype = tika.detect(fileToIdentify);
				//long endTime = new Date().getTime();
				//result.append(filename + "," + filetype + "," + (endTime-startTime)+ HadoopConstants.NEW_LINE);
				result.append(filename + "," + filetype + HadoopConstants.NEW_LINE);
			} catch ( Exception e ) {
				StackTraceElement[] stackTrace = e.getStackTrace();
				StringBuffer stackBuffer = new StringBuffer();
				for (int i=0; i<stackTrace.length; i++) {
					stackBuffer.append(stackTrace[i] + HadoopConstants.NEW_LINE);
				}
				result.append(filename + "," + "EXCEPTION " + e.getClass() + " " + stackBuffer + HadoopConstants.NEW_LINE);
			} 
			
			//output.collect(new Text(key.toString()), new Text(filename + "," + filetype + HadoopConstants.NEW_LINE));
			output.collect(new Text(key.toString()), new Text(result.toString()));
			System.out.println("TikaIdentifierMapper completed");
		}
		
	}
	
	
	public static class TikaIdentifierReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			System.out.println("TikaIdentifierReducer : reduce() - Key=" + key.toString());
			StringBuffer tikaResults = new StringBuffer();
			
			while (values.hasNext()) {
				tikaResults.append(values.next().toString());
			}
			output.collect(new Text(""), new Text(tikaResults.toString()));
			System.out.println("TikaIdentifierReducer : reduce() - Completed");
			
		}
	}
	
	public int run(String[] args) throws Exception {
		
		JobConf jobconf = new JobConf(TikaEvaluatorHadoopMain.class);
		
		FileInputFormat.setInputPaths(jobconf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobconf, new Path(args[1]));

		jobconf.setJobName("Tika Evaluator");	
		jobconf.setJarByClass(TikaEvaluatorHadoopMain.class);
		jobconf.setMapperClass(TikaIdentifierMapper.class);
		jobconf.setCombinerClass( TikaIdentifierReducer.class);
		jobconf.setReducerClass( TikaIdentifierReducer.class);
		jobconf.setOutputKeyClass(Text.class);
		jobconf.setOutputValueClass(Text.class);
		jobconf.setInputFormat(TextInputFormat.class);
		jobconf.setOutputFormat(TextOutputFormat.class);	
		jobconf.setMapOutputKeyClass(Text.class);
		jobconf.setOutputValueClass(Text.class);
		jobconf.set("mapred.map.child.java.opts", "-Xmx512m");
		jobconf.set("mapred.reduce.child.java.opts", "-Xmx512m");
		
		try {
			JobClient.runJob(jobconf);
		} catch (Exception e) {
			System.out.println("Job did not complete successfully - " + e.getMessage() + "");
			return 1;
		}
		System.out.println("Job completed successfully");
		return 0;
	}	
	
	/**
	 * Main method for the Hadoop version of the Electoral Register App
	 * 
	 * @param argsList	List of arguments, properties file and files to process
	 * @throws Exception
	 */
	public static void main(String[] argsList) throws Exception {
		
		System.out.println("Start of TikaEvaluatorHadoop Main");
		/**
		 * Validate number of arguments
		 */
		if (argsList.length < 1) {
			System.out.println("Usage : A file name, directory name or list of files/directories must be supplied");
			System.exit(1);
		}
		ArrayList<String> inputFileList = new ArrayList<String>();
		for (int i=0; i<argsList.length; i++) {
			inputFileList.add(argsList[i]);
		}
		
		/**
		 * Generate the list of files to identify and write to a file
		 */
		FileLister fileLister = new FileLister(inputFileList);
		ArrayList<String> filesToIdentify = fileLister.getListOfFiles();
		for (String filename : filesToIdentify) {
			System.out.println("File " + filename);
		}
		Utilities.createFileListFile(HadoopConstants.FILE_LIST_FILE , filesToIdentify);
		
		// Set up the local Hadoop file system
		boolean local = false;
		// If using Hadoop in local (standalone) mode 
		FileSystem localfs = null;
		FileSystem hdfs = null;
		Path hadoopInputDir = new Path(HadoopConstants.HADOOP_LOCAL_INPUT);
		Path hadoopOutputDir = new Path(HadoopConstants.HADOOP_LOCAL_OUTPUT);
		if (local) {
			System.out.println("Setting up Hadoop local file system");
			localfs = FileSystem.getLocal(new Configuration());
			System.out.println("Hadoop input dir = " + HadoopConstants.HADOOP_LOCAL_INPUT);
			if (!localfs.exists(hadoopInputDir)) {
				System.out.println("Hadoop input dir doesn't exist, creating it");
				localfs.create(hadoopInputDir);
			}
			System.out.println("Hadoop output dir = " + HadoopConstants.HADOOP_LOCAL_OUTPUT);
			if (localfs.exists(hadoopOutputDir)) {
				System.out.println("Hadoop output dir exists, deleting it");
				localfs.delete(hadoopOutputDir, true);
			}
			File inputFileListFile = new File(HadoopConstants.FILE_LIST_FILE);
			Path inputFileListDir = new Path(inputFileListFile.getPath());
			System.out.println("Copying " + inputFileListDir + " to " + hadoopInputDir);
			localfs.copyFromLocalFile(false, true, inputFileListDir, hadoopInputDir);
			FileStatus[] hadoopInputFiles = localfs.listStatus(hadoopInputDir);
			for (int i=0; i<hadoopInputFiles.length; i++) {
				System.out.println("Hadoop input files " + hadoopInputFiles[i].getPath().getName());
			}
		} else {
			System.out.println("Setting up Hadoop HDFS file system");
			hdfs = FileSystem.get(new Configuration());
			System.out.println("Hadoop input dir = " + HadoopConstants.HADOOP_LOCAL_INPUT);
			if (!hdfs.exists(hadoopInputDir)) {
				System.out.println("Hadoop input dir doesn't exist, creating it");
				hdfs.mkdirs(hadoopInputDir);
			}
			System.out.println("Hadoop output dir = " + HadoopConstants.HADOOP_LOCAL_OUTPUT);
			if (hdfs.exists(hadoopOutputDir)) {
				System.out.println("Hadoop output dir exists, deleting it");
				hdfs.delete(hadoopOutputDir, true);
			}
			File inputFileListFile = new File(HadoopConstants.FILE_LIST_FILE);
			Path inputFileListDir = new Path(inputFileListFile.getPath());
			System.out.println("Copying " + inputFileListDir + " to " + hadoopInputDir);
			hdfs.copyFromLocalFile(false, true, inputFileListDir, hadoopInputDir);
			FileStatus[] hadoopInputFiles = hdfs.listStatus(hadoopInputDir);
			for (int i=0; i<hadoopInputFiles.length; i++) {
				System.out.println("Hadoop input files " + hadoopInputFiles[i].getPath().getName());
			}
		}

		/**
		 *  Run the Hadoop map/reduce process
		 */
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SS");
		System.out.println("TikaEvaluatorHadoopMain map/reduce starting at " + dateFormat.format(new Date()));
		Configuration conf = new Configuration();
		String[] hadoopArgsList = {HadoopConstants.HADOOP_LOCAL_INPUT, HadoopConstants.HADOOP_LOCAL_OUTPUT};
		long startTime = new Date().getTime();
		int res = ToolRunner.run(conf, new TikaEvaluatorHadoopMain(), hadoopArgsList);
		long endTime = new Date().getTime();
		System.out.println("TikaEvaluatorHadoopMain map/reduce completed at " + dateFormat.format(new Date()) + " - result=" + res);
			
		/**
		 * Copy the output from the reduce process to the results directory
		 */
		FileStatus[] hadoopOutputFiles = hdfs.listStatus(hadoopOutputDir);
		for (int i=0; i<hadoopOutputFiles.length; i++) {
			String hadoopOutputFileName = hadoopOutputFiles[i].getPath().getName();
			System.out.println("Hadoop output files " + hadoopOutputFileName);
			if (hadoopOutputFileName.startsWith(HadoopConstants.HADOOP_LOCAL_OUTPUT_FILE_PREFIX)) {
				Path outputFile = new Path(HadoopConstants.FILE_TYPES_FILE);
				System.out.println("Copying " + hadoopOutputFiles[i].getPath() + " to " + outputFile);				
				hdfs.copyToLocalFile(hadoopOutputFiles[i].getPath(), outputFile);
				break;
			}
		}	
		
		/**
		 * Create the mime type mapping
		 */
		System.out.println("Generating mime types mapper");
		mimeTypeMapper.generateMimeTypeMapper();
		resultsAnalyser.setMimeTypeMapper(mimeTypeMapper);
		System.out.println("ResultsAnalyser has " + resultsAnalyser.getMimeTypeMapper().getGroundTruthsMap().size() + " mime maps");
		
		
		/**
		 * Analyse the results
		 */
		ArrayList<String> report = resultsAnalyser.analyseResults(HadoopConstants.FILE_TYPES_FILE);
		report.add("Summary:" + HadoopConstants.NEW_LINE);
		report.add("Files processed=" + resultsAnalyser.getTotalCount() + HadoopConstants.NEW_LINE); 
		report.add("Successfully identified=" + resultsAnalyser.getSuccessCount() + HadoopConstants.NEW_LINE);
		report.add("Failed to identify=" + resultsAnalyser.getFailCount() + HadoopConstants.NEW_LINE); 
		report.add("Time taken(mS=" + (endTime-startTime) + HadoopConstants.NEW_LINE);
		report.add("No mime type=" + resultsAnalyser.getNoMimeTypeCount() + HadoopConstants.NEW_LINE); 
		report.add("Errors=" + resultsAnalyser.getErrorCount() + HadoopConstants.NEW_LINE);
		Utilities.createReportFile(HadoopConstants.REPORT_FILE, report);
		
		/**
		 * TODO Postprocessing
		 * Add header to output file
		 * Add a formula to calculate total time taken by Tika to identif the files
		 */
		
		System.out.println("End of of TikaEvaluatorHadoop Main");
	}
}
