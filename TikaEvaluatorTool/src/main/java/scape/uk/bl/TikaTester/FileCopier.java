package scape.uk.bl.TikaTester;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;
import java.util.zip.*;

public class FileCopier {

	/**
	 * @param args
	 */
	
	public static int counter = 0;
	
	public static void main(String[] args) throws IOException {

		//if (args.length < 2) {
		//	System.out.println("Usage: destination, list of files/directories to copy");
		//	System.exit(1);
		//}

		//ArrayList<String> inputFileList = new ArrayList<String>();
		ArrayList<String> filesToIdentify = getListOfFiles("X:\\govdocs\\");
		Utilities.createFileListFile("C:\\Users\\lmarwood\\SCAPE\\Characterisation\\ToolEvaluation\\Results\\FullFileList.txt", filesToIdentify);
		
		//ArrayList<String> zipFiles = getContentsOfZip("X:\\govdocs\\751.zip");
		//for (String filename: zipFiles) {
		//	System.out.println("Found " + filename + " in 751.zip");
		//}
		
		
		//scp /cygdrive/q/govdocs/021/* /cygdrive/q/govdocs/022/* dpt@192.168.244.27:/home/dpt/TikaToolTester
		//StringBuffer buffer = new StringBuffer("scp /cygdrive/q/govdocs/");
		//for (int i=23; i<=73; i++) {
		//	String dirName = "000" + Integer.toString(i);
		//	dirName = dirName.substring(dirName.length()-3);
		//	buffer.append("/cygdrive/q/govdocs/" + dirName + "/* ");
		//}
		//buffer.append("dpt@192.168.244.27:/home/dpt/TikaToolTester/TestFiles/");
		
		///System.out.println(buffer.toString());
		/**
		String inputDirRoot = "X:\\govdocs\\";
		File outputDir = new File("C:\\Users\\lmarwood\\shared\\TikaToolTester\\MoreTestFiles\\");
		for (int i=13; i<=18; i++) {
			//Integer x = new Integer(i);
			String dirName = "000" + Integer.toString(i);
			dirName = dirName.substring(dirName.length()-3);
			String inputDirectory = inputDirRoot + dirName + "\\";
			System.out.println("Directory is " + inputDirectory);
			File testFileDir = new File(inputDirectory);

			String[] fileList = testFileDir.list();
			System.out.println("Directory " + inputDirectory + " contains " + fileList.length +  " files");
			for (int j=0; j<fileList.length; j++) {
				String filename = inputDirectory + fileList[j];
				try {
					FileUtils.copyFileToDirectory(new File(filename), outputDir);
					System.out.println("Copied " + fileList[j] + " to " + outputDir.getPath());
				} catch (IOException ioe) {
					System.out.println("Failed to copy " + fileList[j] + " to " + outputDir.getPath());
					ioe.printStackTrace();
					System.exit(4);
				}
			}
		}
	*/
		/**
		String destination = args[0];
		ArrayList<String> inputFileList = new ArrayList<String>();
		for (int i=1; i<args.length; i++) {
			inputFileList.add(args[i]);
		}
		FileLister fileLister = new FileLister(inputFileList);
		ArrayList<String> fileList = fileLister.getListOfFiles();
		int counter = 0;
		for (String filename: fileList) {
			counter++;
			System.out.println("" + counter + " " + filename);
		}
		**/
		//ArrayList<String> filesToIdentifyHDFS = new ArrayList();
				
		// Copy the files to identify into HDFS
		// for (String filename : filesToIdentify) {
		//	System.out.println("Copying " + filename + " to " + filesToIdentifyDir.getName());
		//	Path fileToIdentify = new Path(filename);
		//	hdfs.copyFromLocalFile(false, true, fileToIdentify, filesToIdentifyDir);
		//}
		
		

	}

    
    private static ArrayList<String> getListOfFiles(String inputDir) throws IOException, FileNotFoundException { 
    	ArrayList<String> fileNameList = new ArrayList<String>();
		File inputFile = new File (inputDir);

		if (inputFile.isDirectory()) {
			File[] filesInFolder = inputFile.listFiles();
			for (int i = 0; i < filesInFolder.length; i++) {
				if (filesInFolder[i].isFile()) {
					counter++;
					System.out.println("  File " + filesInFolder[i].getPath() + " added to list  [" + counter + "]");
					if (filesInFolder[i].getName().endsWith(".zip") || filesInFolder[i].getName().endsWith(".ZIP") ) {
						System.out.println("  File " + filesInFolder[i].getPath() + " is a zip file");
						fileNameList.addAll(getContentsOfZip(filesInFolder[i].getPath()));			
					} else {					
						fileNameList.add(filesInFolder[i].getName());
					}
				} else if (filesInFolder[i].isDirectory()) {
					System.out.println("  File " + filesInFolder[i].getPath() + " is a directory, getting files");
					fileNameList.addAll(getFilesInFolder(filesInFolder[i]));
				} else {
					System.out.println("  Value " + filesInFolder[i].getPath() + " is not a file or directory and will be ignored");
				}
			}
		} else {
			System.out.println("  Value " + inputFile.getPath() + " is not a directory and will be ignored");
		}	
        return fileNameList;
    }
    
    private static ArrayList<String> getFilesInFolder(File folder) {
    	
		File[] filesInFolder = folder.listFiles();
		ArrayList<String> fileNameList = new ArrayList<String>();
		
		for (int i = 0; i < filesInFolder.length; i++) {
			if (filesInFolder[i].isFile()) {
				counter++;
				System.out.println("  File " + filesInFolder[i].getPath() + " added to list  [" + counter + "]");
				fileNameList.add(filesInFolder[i].getPath());
			} else if (filesInFolder[i].isDirectory()) {
				System.out.println("  File " + filesInFolder[i].getPath() + " is a directory, getting files");
				fileNameList.addAll(getFilesInFolder(filesInFolder[i]));
			} else {
				System.out.println("  Value " + filesInFolder[i].getPath() + " is not a file or directory and will be ignored");
			}
		}   	
    	return fileNameList;
    }

    public static ArrayList<String> getContentsOfZip(String zipFile) throws IOException, FileNotFoundException {
    	
		ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
    	ZipEntry ze = zis.getNextEntry();
    	ArrayList<String> contentsOfZip = new ArrayList<String>();
    	 
    	while (ze != null){
    	   String fileName = ze.getName();
    	   if (!fileName.endsWith("/")) {
    		   fileName = ze.getName().substring(ze.getName().lastIndexOf("/")+1);
    		   contentsOfZip.add(fileName);
    		   counter++;
    	   	   System.out.println("  Zipped file " + fileName + " added to list  [" + counter + "]");
    	   } else {
    		   System.out.println("  Zipped file " + fileName + " is a directory, ignoring");
    	   } 
    	   ze = zis.getNextEntry();
    	}
 
        zis.closeEntry();
    	zis.close();
    	return contentsOfZip;
    }
}
