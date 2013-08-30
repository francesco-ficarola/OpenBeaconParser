package it.uniroma1.dis.wsngroup.parsing;

import it.uniroma1.dis.wsngroup.constants.ParsingConstants;
import it.uniroma1.dis.wsngroup.core.realtime.RealTimeLogFileTailer;
import it.uniroma1.dis.wsngroup.core.realtime.RealTimeLogFileTailerListener;
import it.uniroma1.dis.wsngroup.parsing.modules.realtime.RealTimeModule;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

/**
 * @author Francesco Ficarola
 *
 */

public class RealTimeParser implements RealTimeLogFileTailerListener {

	private static Logger logger = Logger.getLogger(RealTimeParser.class);
	
	private static FileInputStream fis;
	private static File fileInput;
	private static Integer startTS;
	private static Integer endTS;
	private static GraphRepresentation graph;
	private static boolean createVL;
	private RealTimeLogFileTailer tailer;
	
	public RealTimeParser() {
		startTS = 0;
		endTS = 2147483647;
		graph = null;
		createVL = false;
		tailer = new RealTimeLogFileTailer( fileInput, ParsingConstants.REAL_TIME_REFRESH_INTERVAL, false );
		tailer.addRealTimeLogFileTailerListener( this );
		tailer.start();
	}
	
	public static void main(String[] args) {
		
		logger.info("Initializing...");
		
		try {
			while(init()) {
				;
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		}
	}
	
	private static boolean init() throws IOException {
		boolean missingfile = true;
		
		System.out.print("File to read: ");
		BufferedReader key = new BufferedReader(new InputStreamReader(System.in));

		fileInput = new File(key.readLine());
		
		if(fileInput.exists()) {
			missingfile = false;
			fis = new FileInputStream(fileInput);
			new RealTimeParser();
			
			System.out.print("Would you like to create virtual links? [y/N] ");
			if(key.readLine().equals("y")) {
				createVL = true;
			}
			
			graph = new RealTimeModule(fileInput, fis, createVL);
			
		} else {
			System.out.println("ERROR: " + fileInput.getAbsolutePath()+ "- No such file or directory");
		}
		
		return missingfile;
	}
	
	public void newLogFileLine(String line) {
		try {
			graph.readLog(startTS, endTS);
		} catch (IOException e) {
			logger.error(e.getMessage());
//			e.printStackTrace();
		}
	}

}
