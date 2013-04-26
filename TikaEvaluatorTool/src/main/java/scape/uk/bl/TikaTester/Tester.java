package scape.uk.bl.TikaTester;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import scape.uk.bl.ResultsAnalysis.MimeTypeMapper;
import scape.uk.bl.ResultsAnalysis.ResultsAnalyser;

public class Tester {

	public static MimeTypeMapper mimeTypeMapper;
	public static ResultsAnalyser resultsAnalyser;
	public static int lineCounter=0; 
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException, FileNotFoundException {
		// TODO Auto-generated method stub
		System.out.println("Start of Tester Main");
		
		String filename = "123456.doc";
		String fileId = filename.substring(0, filename.indexOf("."));
		System.out.println("fileId = [" + fileId + "]");

		/**
		String filename = "C:\\Users\\lmarwood\\shared\\TikaToolTester\\TestFilesSmall\\000003.doc";
		String fileType = "application/msword";
		
		 * Create the mapper object - pass in the mime mapper file
		 
		mimeTypeMapper = new MimeTypeMapper(Constants.MIME_MAPPER_FILE);
		mimeTypeMapper.generateMimeTypeMapper();
		resultsAnalyser = new ResultsAnalyser(mimeTypeMapper);
		String resultOfAnalysis = Tester.resultsAnalyser.analyseResult(filename + "," + fileType);
		System.out.println("Result Of Analysis=" + resultOfAnalysis);
		System.out.println("End of of Tester Main");
		*/
		
		InputStream propertiesInputStream = Tester.class.getClassLoader().getResourceAsStream("TikaEvaluator.properties");
		System.out.println("propertiesInputStream = " + propertiesInputStream);
		Properties prop = new Properties();
		prop.load(propertiesInputStream);
		System.out.println(prop.getProperty("FILE_TYPES_FILE"));
		System.out.println("Start of Tester Main");
	}

}
