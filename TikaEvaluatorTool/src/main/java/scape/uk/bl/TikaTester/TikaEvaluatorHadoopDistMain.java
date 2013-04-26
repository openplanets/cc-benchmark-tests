package scape.uk.bl.TikaTester;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tika.Tika;

import scape.uk.bl.ResultsAnalysis.MimeTypeMapper;
import scape.uk.bl.ResultsAnalysis.ResultsAnalyser;

public class TikaEvaluatorHadoopDistMain extends Configured implements Tool {

	public static class TikaIdentifierDistMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
    	private Tika tika = new Tika();
    	
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			StringBuffer result = new StringBuffer("");
			System.out.println("TikaIdentifierMapper : map() - Key=" + key.toString() + ", Value=" + value.toString());	
	    	
			
			// Create temporary directory
			//File localTempDir = Utilities.newTempDir();
			//System.out.println("TikaIdentifierMapper : created temporary directory " + localTempDir.getAbsolutePath());
			
			String filename = value.toString();
			//File fileToIdentify = new File(filename);
			
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			
			// Pass input stream to Tika rather than copying file into temp dir
			
			Path fileToIdentify = new Path(HadoopConstants.HADOOP_LOCAL_INPUT + "//" + HadoopConstants.HADOOP_FILES_TO_IDENTIFY + "//" + filename);
			fs.open(fileToIdentify);
			long startTime = new Date().getTime();
			String filetype = tika.detect(fs.open(fileToIdentify));
			long endTime = new Date().getTime();
			
			
			
			
			
			
			
			// Copy file to identify into the temporary directory
			//File fileToIdentify = Utilities.copyInputFileToLocalTemp(localTempDir, fs, 
			//	 HadoopConstants.HADOOP_LOCAL_INPUT + "/" + HadoopConstants.HADOOP_FILES_TO_IDENTIFY + "/" + filename);
			//System.out.println("TikaIdentifierMapper : file to identify is " + fileToIdentify);
			
			// Pass file to Tika for identification 
			//long startTime = new Date().getTime();
			//String filetype = tika.detect(fileToIdentify);
			//long endTime = new Date().getTime();

			//System.out.println("TikaIdentifierMapper : deleting temporary directory " + localTempDir.getPath());
			//if (localTempDir != null && localTempDir.exists()) {
			//	Utilities.deleteDirectory(localTempDir);
			//}
			
			
			result.append(filename + "," + filetype + "," + (endTime-startTime) + HadoopConstants.NEW_LINE);

			output.collect(new Text(key.toString()), new Text(result.toString()));
			System.out.println("TikaIdentifierMapper completed");
		}
		
	}
	
	public static class TikaIdentifierDistReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

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
		
		JobConf jobconf = new JobConf(TikaEvaluatorHadoopDistMain.class);
		
		FileInputFormat.setInputPaths(jobconf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobconf, new Path(args[1]));

		jobconf.setJobName("Tika Evaluator");	
		jobconf.setJarByClass(TikaEvaluatorHadoopDistMain.class);
		jobconf.setMapperClass(TikaIdentifierDistMapper.class);
		jobconf.setCombinerClass( TikaIdentifierDistReducer.class);
		jobconf.setReducerClass( TikaIdentifierDistReducer.class);
		jobconf.setOutputKeyClass(Text.class);
		jobconf.setOutputValueClass(Text.class);
		jobconf.setInputFormat(TextInputFormat.class);
		jobconf.setInputFormat(NLineInputFormat.class);
		jobconf.setInt("mapred.line.input.format.linespermap", 20);
		jobconf.setOutputFormat(TextOutputFormat.class);	
		jobconf.setMapOutputKeyClass(Text.class);
		jobconf.setOutputValueClass(Text.class);
		//jobconf.set("mapred.map.child.java.opts", "-Xmx512m");
		//jobconf.set("mapred.reduce.child.java.opts", "-Xmx512m");
		
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
	 * Main method for the fully distributed Hadoop version of the Electoral Register App
	 * 
	 * @param argsList	List of arguments, properties file and files to process
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] argsList) throws Exception {
		
		System.out.println("Start of TikaEvaluatorHadoopDistMain");
		/**
		 * Validate number of arguments
		 */
		//if (argsList.length < 1) {
		//	System.out.println("Usage : A file name, directory name or list of files/directories must be supplied");
		//	System.exit(1);
		//}
		//ArrayList<String> inputFileList = new ArrayList<String>();
		//for (int i=0; i<argsList.length; i++) {
		//	inputFileList.add(argsList[i]);
		//}
		
		
		// Set up the local Hadoop file system
		FileSystem hdfs = null;
		Path hadoopInputDir = new Path(HadoopConstants.HADOOP_LOCAL_INPUT);
		Path hadoopOutputDir = new Path(HadoopConstants.HADOOP_LOCAL_OUTPUT);
		System.out.println("Setting up Hadoop HDFS file system");
		hdfs = FileSystem.get(new Configuration());
		
		// Check that the Hadoop input directory exists, if not create it
		//System.out.println("Hadoop input dir = " + HadoopConstants.HADOOP_LOCAL_INPUT);
		//if (!hdfs.exists(hadoopInputDir)) {
		//	System.out.println("Hadoop input dir doesn't exist, creating it");
		//	hdfs.mkdirs(hadoopInputDir);
		//}
		//Path filesToIdentifyDir = new Path(HadoopConstants.HADOOP_LOCAL_INPUT + "/" + HadoopConstants.HADOOP_FILES_TO_IDENTIFY);
		//if (!hdfs.exists(filesToIdentifyDir)) {
		//	System.out.println("Hadoop files to identify dir doesn't exist, creating it");
		//	hdfs.mkdirs(filesToIdentifyDir);
		//}
		
		// Check that the Hadoop output directory exists, if it does delete it
		System.out.println("Hadoop output dir = " + HadoopConstants.HADOOP_LOCAL_OUTPUT);
		if (hdfs.exists(hadoopOutputDir)) {
			System.out.println("Hadoop output dir exists, deleting it");
			hdfs.delete(hadoopOutputDir, true);
		}
		
		/**
		 * Generate the list of files to identify and copy into HDFS
		 */
		//FileLister fileLister = new FileLister(inputFileList);
		//ArrayList<String> filesToIdentify = fileLister.getListOfFiles();
		//ArrayList<String> filesToIdentifyHDFS = new ArrayList();
		
		// Copy the files to identify into HDFS
		//for (String filename : filesToIdentify) {
		//	System.out.println("Copying " + filename + " to " + filesToIdentifyDir.getName());
		//	Path fileToIdentify = new Path(filename);
		//	hdfs.copyFromLocalFile(false, true, fileToIdentify, filesToIdentifyDir);
		//}
			
		// Create the file containing the list of files to identify & copy it to the Hadoop input directory
		//FileStatus[] hadoopFilesToIdentify = hdfs.listStatus(filesToIdentifyDir);
		//for (int i=0; i<hadoopFilesToIdentify.length; i++) {
		//	System.out.println("Hadoop files to identify :" + hadoopFilesToIdentify[i].getPath().getName());
		//	filesToIdentifyHDFS.add(hadoopFilesToIdentify[i].getPath().getName());
		//}
		//Utilities.createFileListFile(HadoopConstants.FILE_LIST_FILE, filesToIdentifyHDFS);
		//File inputFileListFile = new File(HadoopConstants.FILE_LIST_FILE);
		//Path inputFileListDir = new Path(inputFileListFile.getPath());
		//System.out.println("Copying " + inputFileListDir + " to " + hadoopInputDir);
		//hdfs.copyFromLocalFile(false, true, inputFileListDir, hadoopInputDir);
		//FileStatus[] hadoopInputFiles = hdfs.listStatus(hadoopInputDir);
		//for (int i=0; i<hadoopInputFiles.length; i++) {
		//	System.out.println("Hadoop input files " + hadoopInputFiles[i].getPath().getName());
		//}

		/**
		 *  Run the Hadoop map/reduce process
		 */
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SS");
		System.out.println("TikaEvaluatorHadoopDistMain map/reduce starting at " + dateFormat.format(new Date()));
		Configuration conf = new Configuration();
		String[] hadoopArgsList = {HadoopConstants.HADOOP_LOCAL_INPUT + "/" + "FileList.txt", HadoopConstants.HADOOP_LOCAL_OUTPUT};
		long startTime = new Date().getTime();
		int res = ToolRunner.run(conf, new TikaEvaluatorHadoopDistMain(), hadoopArgsList);
		long endTime = new Date().getTime();
		System.out.println("TikaEvaluatorHadoopDistMain map/reduce process took " + (endTime-startTime) + "mS");

		/**
		if (res == 0) {
			System.out.println("TikaEvaluatorHadoopDistMain map/reduce completed successfully at " + dateFormat.format(new Date()) + " - result=" + res);
			/**
			 * Copy the output from the reduce process to the results directory
			 *
			FileStatus[] hadoopOutputFiles = hdfs.listStatus(hadoopOutputDir);
			int totalLines=0;
			boolean appendToResultsFile = false;
			for (int i=0; i<hadoopOutputFiles.length; i++) {
				Map<String, ArrayList<String>> fileTypes = new HashMap<String, ArrayList<String>>();
				int lineCounter=0;
				String hadoopOutputFileName = hadoopOutputFiles[i].getPath().getName();
				if (hadoopOutputFiles[i].getLen() > 0 && hadoopOutputFileName.startsWith(HadoopConstants.HADOOP_LOCAL_OUTPUT_FILE_PREFIX)) {
					System.out.println("Processing Hadoop output file " + hadoopOutputFileName);
					FSDataInputStream is = hdfs.open(hadoopOutputFiles[i].getPath());
					try {
					    String text = null;
					    while ((text = is.readLine()) != null) {
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
					    if (is != null) is.close(); 
					    System.out.println("Completed processing of Hadoop output file " + hadoopOutputFileName + ", " + lineCounter + " lines");
					    totalLines+=lineCounter;
					    Utilities.createResultsFile(HadoopConstants.FILE_TYPES_FILE, fileTypes, appendToResultsFile);
						fileTypes = null;
						appendToResultsFile = true;
					}
				}
			}	
			System.out.println("Completed processing of all Hadoop output files, total lines=" + totalLines + " lines");
			*/
			
			/**
			 * Create the mime type mapping
			 */
			//MimeTypeMapper mimeTypeMapper = new MimeTypeMapper(HadoopConstants.MIME_MAPPER_FILE);
			//ResultsAnalyser resultsAnalyser = new ResultsAnalyser(mimeTypeMapper);
			//System.out.println("Generating mime types mapper");
			//mimeTypeMapper.generateMimeTypeMapper();
			//resultsAnalyser.setMimeTypeMapper(mimeTypeMapper);
			//System.out.println("ResultsAnalyser has " + resultsAnalyser.getMimeTypeMapper().getGroundTruthsMap().size() + " mime maps");
			
			/**
			 * Analyse the results
			 */
			//System.out.println("Analysing results");
			//ArrayList<String> report = resultsAnalyser.analyseResults(HadoopConstants.FILE_TYPES_FILE);
			//report.add("Summary:" + HadoopConstants.NEW_LINE);
			//report.add("Files processed=" + resultsAnalyser.getTotalCount() + HadoopConstants.NEW_LINE); 
			//report.add("Successfully identified=" + resultsAnalyser.getSuccessCount() + HadoopConstants.NEW_LINE);
			//report.add("Failed to identify=" + resultsAnalyser.getFailCount() + HadoopConstants.NEW_LINE); 
			//report.add("No mime type=" + resultsAnalyser.getNoMimeTypeCount() + HadoopConstants.NEW_LINE); 
			//report.add("Time taken(mS)=" + (endTime-startTime) + HadoopConstants.NEW_LINE);
			//report.add("Errors=" + resultsAnalyser.getErrorCount() + HadoopConstants.NEW_LINE);
			//Utilities.createReportFile(HadoopConstants.REPORT_FILE, report);
		//} else {
		//	System.out.println("TikaEvaluatorHadoopDistMain map/reduce failed at " + dateFormat.format(new Date()) + " - result=" + res);
		//}
		
		/**
		 * TODO Postprocessing
		 * Add header to output file
		 * Add a formula to calculate total time taken by Tika to identif the files
		 */
		
		System.out.println("End of of TikaEvaluatorHadoopDistMain");
	}
}
