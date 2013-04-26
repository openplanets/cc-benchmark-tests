package scape.uk.bl.TikaTester;

public class HadoopConstants {

	public static final String TIKA_TESTER_ROOT		= "/home/dpt/TikaToolTester/";
	public static final String FILE_TYPES_FILE 		= TIKA_TESTER_ROOT + "Results/TikaResults.csv";  
	public static final String GROUNDTRUTHS_FILE 	= TIKA_TESTER_ROOT + "GroundTruths/complete.csv";
	public static final String MIME_TYPES_FILE 		= TIKA_TESTER_ROOT + "GroundTruths/mimetype-decoder-new.csv";
	public static final String REPORT_FILE 			= TIKA_TESTER_ROOT + "Results/Report.csv";
	public static final String MIME_MAPPER_FILE 	= TIKA_TESTER_ROOT + "GroundTruths/MimeMapper.csv";
	public static final String FILE_LIST_FILE 		= TIKA_TESTER_ROOT + "Results/FileList.txt";
	public static final String HADOOP_LOCAL_INPUT 	= "TikaEvaluatorInput";
	public static final String HADOOP_LOCAL_OUTPUT 	= "TikaEvaluatorOutput";
	public static final String HADOOP_FILES_TO_IDENTIFY 	= "FilesToIdentify";
	public static final String HADOOP_LOCAL_OUTPUT_FILE_PREFIX = "part-";	
	public static final String HADOOP_TEMP_INPUT_DIR = "/tmp/hadooptmp-tikaEval/";
	public static final String HADOOP_JOB_NAME = "TikaEvaluator";
	public static final int BUFFER_SIZE = 32768;
	public static final String NEW_LINE = "\n";
}
