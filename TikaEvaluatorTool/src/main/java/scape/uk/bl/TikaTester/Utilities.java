package scape.uk.bl.TikaTester;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import scape.uk.bl.ResultsAnalysis.GroundTruth;

public class Utilities {

	public static void createResultsFile(String fileName, Map<String, String> fileTypes, String startTime, String endTime) throws IOException {	
		
		File fileTypesFile = new File(fileName);
		Writer fileListBuffer = new BufferedWriter(new FileWriter(fileTypesFile));
		//fileListBuffer.write("Input File, File Type\r\n");
		//fileListBuffer.flush();
		
		Iterator it = fileTypes.entrySet().iterator();
		StringBuffer outBuffer = new StringBuffer("");
	    while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();
	        //System.out.println(fileTypePairs.getKey() + " = " + fileTypePairs.getValue());
	        
	        outBuffer.append("\"");
	        outBuffer.append(fileTypePairs.getKey());
	        outBuffer.append("\",\"");
	        outBuffer.append(fileTypePairs.getValue());
	        outBuffer.append("\"\r\n");
	        fileListBuffer.write(outBuffer.toString());
	        outBuffer.setLength(0);
	    }
	    outBuffer.append("Summary: Started ");
	    outBuffer.append(startTime);
	    outBuffer.append("  Completed ");
	    outBuffer.append(endTime);
	    outBuffer.append("\r\n");
	    fileListBuffer.write(outBuffer.toString());
		fileListBuffer.flush();
		fileListBuffer.close();
	}
	
	public static void createResultsFile(String resultsFileName, Map<String, ArrayList<String>> fileTypes) throws IOException {	
	
		createResultsFile(resultsFileName, fileTypes, false);
		
	}
	
	
	public static void createResultsFile(String resultsFileName, Map<String, ArrayList<String>> fileTypes, boolean appendToExisting) throws IOException {	
		
		File fileTypesFile = new File(resultsFileName);
		Writer fileListBuffer = new BufferedWriter(new FileWriter(fileTypesFile, appendToExisting));
		if (!appendToExisting) {
			fileListBuffer.write("Input File, File Type, Elapsed Time" + Constants.NEW_LINE);
			fileListBuffer.flush();
		}
		
		Iterator it = fileTypes.entrySet().iterator();
		StringBuffer outBuffer = new StringBuffer("");
	    while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();
	        //System.out.println(fileTypePairs.getKey() + " = " + fileTypePairs.getValue());
	        
	        outBuffer.append("\"");
	        outBuffer.append(fileTypePairs.getKey());
        	outBuffer.append("\",");
	        ArrayList<String> fileDetails = (ArrayList<String>) fileTypePairs.getValue();
	        for (String fileDetail: fileDetails) {
	        	outBuffer.append("\"");
	        	outBuffer.append(fileDetail);
	        	outBuffer.append("\",");
	        }
	        outBuffer.setLength(outBuffer.length()-1);
	        outBuffer.append(Constants.NEW_LINE);
	        fileListBuffer.write(outBuffer.toString());
	        outBuffer.setLength(0);
	    }
		fileListBuffer.flush();
	    fileListBuffer.close();
	}
	
	public static void updateResultsFile(String resultsFileName, Map<String, ArrayList<String>> fileTypes) throws IOException {	
		
		File fileTypesFile = new File(resultsFileName);
		Writer fileListBuffer = new BufferedWriter(new FileWriter(fileTypesFile, true));
		
		Iterator it = fileTypes.entrySet().iterator();
		StringBuffer outBuffer = new StringBuffer("");
	    while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();        
	        outBuffer.append("\"");
	        outBuffer.append(fileTypePairs.getKey());
        	outBuffer.append("\",");
	        ArrayList<String> fileDetails = (ArrayList<String>) fileTypePairs.getValue();
	        for (String fileDetail: fileDetails) {
	        	outBuffer.append("\"");
	        	outBuffer.append(fileDetail);
	        	outBuffer.append("\",");
	        }
	        outBuffer.setLength(outBuffer.length()-1);
	        outBuffer.append(Constants.NEW_LINE);
	        fileListBuffer.write(outBuffer.toString());
	        outBuffer.setLength(0);
	    }
	}

	public static void createReportFile(String fileName, ArrayList<String> contents) throws IOException {	
		
		createReportFile(fileName, contents, false);
		
	}
	
	public static void createReportFile(String fileName, ArrayList<String> contents, boolean appendToFile) throws IOException {	
		
		File reportFile = new File(fileName);
		Writer fileListBuffer = new BufferedWriter(new FileWriter(reportFile, appendToFile));
		
		for (String contentLine: contents) {
	        fileListBuffer.write(contentLine  + Constants.NEW_LINE);
		}
		fileListBuffer.flush();
		fileListBuffer.close();
	}
	
	public static void createMimeTypeMapperFile(String fileName, Map<String, GroundTruth> mappingData) throws IOException {	
		
		File mimeMapperFile = new File(fileName);
		Writer fileBuffer = new BufferedWriter(new FileWriter(mimeMapperFile));
		StringBuffer outputBuffer = new StringBuffer("");
		
		Iterator<Entry<String, GroundTruth>> it = mappingData.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry fileTypePairs = (Map.Entry)it.next();
	        System.out.println("  " + fileTypePairs.getKey() + "=" + 
	        		((GroundTruth)fileTypePairs.getValue()).getDescription() + ",  Mime types=" +
	        		((GroundTruth)fileTypePairs.getValue()).getMimeTypes());
	        
	        outputBuffer.append("\"");
	        outputBuffer.append(fileTypePairs.getKey());
	        outputBuffer.append("\",\"");
	        outputBuffer.append(((GroundTruth)fileTypePairs.getValue()).getDescription());
	        outputBuffer.append("\",\"");
	        ArrayList<String> mimeTypes = ((GroundTruth)fileTypePairs.getValue()).getMimeTypes();
	        StringBuffer mimeTypeList = new StringBuffer("");
	        if (mimeTypes != null) {
	        	for (String mimeType: mimeTypes) {
	        		mimeTypeList.append(mimeType);
	        		mimeTypeList.append(";");
	        	}
	        	mimeTypeList.setLength(mimeTypeList.length()-1);
	        }
        	outputBuffer.append(mimeTypeList);
        	outputBuffer.append("\"\r\n");
        	fileBuffer.write(outputBuffer.toString());
        	outputBuffer.setLength(0);
	    }

	    fileBuffer.flush();
	    fileBuffer.close();
	}
	
	public static void createFileListFile(String fileName, ArrayList<String> contents) throws IOException {	
		createFileListFile(fileName, contents, false);
	}
	
	public static void createFileListFile(String fileName, ArrayList<String> contents, boolean appendToExisting) throws IOException {	
		
		File fileListFile = new File(fileName);
		Writer fileListBuffer = new BufferedWriter(new FileWriter(fileListFile, appendToExisting));
		
		for (String contentLine: contents) {
	        fileListBuffer.write(contentLine + HadoopConstants.NEW_LINE);
		}
		fileListBuffer.flush();
		fileListBuffer.close();
	}
	
	
	/**
	 * Extracts the file name from the file path
	 * @param filePath
	 * @return
	 */
	public static String getFileName(String filePath) {
		filePath = removeQuotes(filePath);
		String filename = new File(filePath).getName();
		return filename;

	}
	
	/**
	 * Remove leading and training quotes from a string
	 * @param quotedText
	 * @return
	 */
	public static String removeQuotes(String quotedText) {
		quotedText = quotedText.trim();
		if (quotedText.startsWith("\"")) {
			quotedText = quotedText.substring(1);
		}
		if (quotedText.endsWith("\"")) {
			quotedText = quotedText.substring(0, quotedText.length()-1);
		}
		return quotedText;
	}
	
	/**
	 * Creates a new temporary directory 
	 * @return File object for new directory
	 * @throws IOException file access error
	 */
	public static File newTempDir() throws IOException {
		//create a temporary local output file name for use with the local tool in TMPDIR
		new File(HadoopConstants.HADOOP_TEMP_INPUT_DIR).mkdirs();
		File localOutputTempDir = File.createTempFile(HadoopConstants.HADOOP_JOB_NAME,"",
				new File(HadoopConstants.HADOOP_TEMP_INPUT_DIR));
		//change this to a directory and put all the files in there
		localOutputTempDir.delete();
		localOutputTempDir.mkdirs();
		localOutputTempDir.setReadable(true, false);
		localOutputTempDir.setWritable(true, false);//need this so output can be saved
		return localOutputTempDir;

	}
	
	/**
	 * Copy an input file to a local temporary file and return the new local filename
	 * @param tmpDir temporary directory to copy files to
	 * @param fs The HDFS filesystem
	 * @param inputFile The URL/name of the input file
	 * @return A File instance for the new local temporary file
	 * @throws IOException file access error
	 */
	public static File copyInputFileToLocalTemp(File tmpDir, FileSystem fs, String inputFile) throws IOException {
		
		//put the file in the new temporary directory
		//we use lastindexof as path may start hdfs:// and File doesn't understand
		System.out.println("Utilities temp dir=" + tmpDir + " input file = " + inputFile);
		System.out.println("Utilities FileSystem=" + fs.toString());
		
		if (!tmpDir.exists()) {
			System.out.println("Utilities - temp dir = " + tmpDir.getPath() + " doesn't exist");
			return null;
		} else if (!tmpDir.isDirectory()) {
			System.out.println("Utilities - temp dir = " + tmpDir.getPath() + " is not a directory");
			return null;
		}
		System.out.println("Utilities - temp dir = " + tmpDir.getPath() + " is a directory");
		
		String tempFile = tmpDir.getAbsolutePath();
		if(inputFile.contains("/")) {
			tempFile+=(inputFile.substring(inputFile.lastIndexOf("/")));
		} else {
			tempFile+="/"+inputFile;
		}
		File tempInputFile = new File(tempFile);
		System.out.println("Utilities temp input file = " + tempInputFile.getPath());
		//if this file has already been copied - skip
		//FIXME: this thinks that the file exists when it doesn't
		if(tempInputFile.exists()) return tempInputFile;
		
		//i.e. this file is a local file
		if(new File(inputFile).exists()) {
			System.out.println("Utilities - copying from local fs");
			FileInputStream fis = new FileInputStream(inputFile);
			FileOutputStream fos = new FileOutputStream(tempInputFile);
			byte[] buffer = new byte[HadoopConstants.BUFFER_SIZE];
			int bytesRead = 0;
			while(fis.available()>0) {
				bytesRead = fis.read(buffer);
				fos.write(buffer, 0, bytesRead);
			}
			fis.close();
			fos.close();
			return tempInputFile;
		}
		
		//this file is in HDFS
		Path inputHDFSFile = new Path(inputFile);
		System.out.println("Utilities - HDFS file = " + inputHDFSFile.toString());
		FileStatus[] hdfsFiles = fs.listStatus(new Path(HadoopConstants.HADOOP_LOCAL_INPUT + "/" + HadoopConstants.HADOOP_FILES_TO_IDENTIFY));
		for (int i=0; i<hdfsFiles.length;  i++) {
			System.out.println("Utilities - HDFS file system = " + hdfsFiles[i].getPath());
		}
		
		if(fs.exists(inputHDFSFile)) {
			System.out.println("Copying from hdfs");
			fs.copyToLocalFile(inputHDFSFile, new Path(tempFile));
			tempInputFile = new File(tempFile);
			System.out.println("Copied from hdfs");
			return tempInputFile;
		}
		//TODO: check for HTTP files etc

		System.out.println("file not found");
		return null;
	}

	public static boolean deleteTempFile(File tempDirectory, String tempFile) throws IOException {

		File tempFilePath = new File(tempDirectory.getPath() + "/" + tempFile);
		System.out.println("Deleting " + tempFilePath.getPath());
		return(tempFilePath.delete());
	}

	public static boolean deleteTempFile(File tempFile) throws IOException {

		return(tempFile.delete());
	}
	
	/**
	 * Recursively delete directory
	 * @param dir directory to delete
	 * @return success
	 */
	public static boolean deleteDirectory(File dir) {
		boolean ret = true;
		for(File f: dir.listFiles()) {
			f.delete();
		}
		ret=dir.delete();
		return ret;
	}

}
