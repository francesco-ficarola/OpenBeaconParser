package it.uniroma1.dis.wsngroup.parsing.modules.dynamics;

import it.uniroma1.dis.wsngroup.parsing.LogParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
//import java.util.Iterator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonModuleWithoutDB extends AbstractModuleWithoutDB {

	private Gson gson;
	
	public JsonModuleWithoutDB(File fileInput, FileInputStream fis, boolean createVL) {
		super(fileInput, fis, createVL);
		if(LogParser.prettyJSON) {
			gson = new GsonBuilder().setPrettyPrinting().create();
		} else {
			gson = new Gson();
		}
	}
	
	public void writeFile(String outputFileName, String separator) throws IOException {
		if(tsObjectList.size() > 0) {
			logger.info("Writing...");
			
			File f = new File(fileInput.getParentFile() + "/" + outputFileName);
			FileOutputStream fos = new FileOutputStream(f, true);
			PrintStream ps = new PrintStream(fos);
			

//			// JSON root header
//			ps.println("{");
//			ps.println("\"timestamps\": [");
//			
//			// Timestamps data
//			Iterator<TimestampObject> it = tsObjectList.iterator();
//			int count = 0;
//			while(it.hasNext()) {
//				if(count != tsObjectList.size()-1) {
//					ps.println(gson.toJson(it.next()) + ",");
//				} else {
//					ps.println(gson.toJson(it.next()));
//				}
//				count++;
//			}
//			
//			// JSON root footer
//			ps.println("]");
//			ps.println("}");

			ps.println(gson.toJson(tsObjectList));
			logger.info("Output file: " + f.getAbsolutePath());
			ps.close();
		}
	}
	
	/*
	public void writeFile(String outputFileName, String separator) throws IOException {
		if(tsObjectList.size() > 0) {
			
			logger.info("Writing...");
			
			Iterator<TimestampObject> it = tsObjectList.iterator();
			
			while(it.hasNext()) {
				TimestampObject timestampObject = it.next();
				File f = new File(fileInput.getParentFile() + "/data/" + "socialdis_" + timestampObject.getTimestamp()  + ".json");
				FileOutputStream fos = new FileOutputStream(f, false);
				PrintStream ps = new PrintStream(fos);
			
				ps.println(gson.toJson(timestampObject));
				ps.close();
			}
		}
	}
	*/
	
	/*
	public void writeFile(String outputFileName, String separator) throws IOException {
		if(tsObjectList.size() > 0) {
			
			logger.info("Writing...");
			
			Iterator<TimestampObject> it = tsObjectList.iterator();
			int count = 0;
			
			while(it.hasNext()) {
				long opStart = System.currentTimeMillis();
				
//				File f = new File(fileInput.getParentFile() + "/" + outputFileName);
				File f = new File(fileInput.getParentFile() + "/" + "socialdis.json");
				FileOutputStream fos = new FileOutputStream(f, false);
				PrintStream ps = new PrintStream(fos);
			
				if(count != tsObjectList.size()-1) {
					ps.println(gson.toJson(it.next()));
					ps.close();
					
					count++;
					logger.debug("Output file: " + f.getAbsolutePath());
					
					try {
						Thread.sleep(ParsingConstants.JSON_SLEEP_INTERVAL-(System.currentTimeMillis()-opStart));
					} catch (InterruptedException e) {
						logger.error(e);
					} catch (IllegalArgumentException e) {
						try {
							Thread.sleep(ParsingConstants.JSON_SLEEP_INTERVAL);
						} catch (InterruptedException e1) {
							logger.error(e);
						}
					}
					
				} else {
					ps.println(gson.toJson(it.next()));
					ps.close();
					
					logger.debug("Output file: " + f.getAbsolutePath());
					
					try {
						Thread.sleep(ParsingConstants.JSON_SLEEP_INTERVAL-(System.currentTimeMillis()-opStart));
					} catch (InterruptedException e) {
						logger.error(e);
					} catch (IllegalArgumentException e) {
						try {
							Thread.sleep(ParsingConstants.JSON_SLEEP_INTERVAL);
						} catch (InterruptedException e1) {
							logger.error(e);
						}
					}
					
					writeFile(outputFileName, separator);
				}				
			}
		}
	}
	*/

}
