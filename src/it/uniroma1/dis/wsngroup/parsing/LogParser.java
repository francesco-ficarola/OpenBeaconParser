package it.uniroma1.dis.wsngroup.parsing;

import it.uniroma1.dis.wsngroup.constants.ParsingConstants;
import it.uniroma1.dis.wsngroup.core.DateTime;
import it.uniroma1.dis.wsngroup.parsing.modules.aggregate.AdjacencyListsModule;
import it.uniroma1.dis.wsngroup.parsing.modules.aggregate.AdjacencyMatrixModule;
import it.uniroma1.dis.wsngroup.parsing.modules.aggregate.DynamicNetworkModule;
import it.uniroma1.dis.wsngroup.parsing.modules.dis.GexfModuleAggregateDIS;
import it.uniroma1.dis.wsngroup.parsing.modules.dis.JsonModuleDIS;
import it.uniroma1.dis.wsngroup.parsing.modules.dynamics.GexfModuleDynamic;
import it.uniroma1.dis.wsngroup.parsing.modules.dynamics.JsonModuleWithoutDB;
import it.uniroma1.dis.wsngroup.parsing.modules.dynamics.NetLogoModule;
import it.uniroma1.dis.wsngroup.parsing.modules.macro.DynamicNetworkModuleMacro;
import it.uniroma1.dis.wsngroup.parsing.modules.macro.GexfModuleAggregateMacro;
import it.uniroma1.dis.wsngroup.parsing.modules.macro.JsonModuleMacro;
import it.uniroma1.dis.wsngroup.parsing.modules.macro.NetLogoModuleMacro;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;


public class LogParser {

	/**
	 * @author Francesco Ficarola
	 *
	 */

	private static Logger logger = Logger.getLogger(LogParser.class);
	
	private static String separator;
	private static String fileType;
	private static int representationType;
	private static FileInputStream fis;
	private static File fileInput;
	private static String[] dateTime;
	private static String listGraphOut;
	private static Integer startTS;
	private static Integer endTS;
	
	public static boolean prettyJSON;
	
	public static Integer buildingEdgeMode;
	public static Integer numberOfContinuousTS;
	public static Integer numberOfIntervalTS;
	public static Integer percOfDeliveredMsg;
	
	public LogParser() {
		fileInput = null;
		separator = ParsingConstants.TAB_SEPARATOR;
		fileType = ParsingConstants.TXT_FILE_TYPE;
		representationType = ParsingConstants.ADJACENCY_LISTS_REPRESENTATION;
		dateTime = new DateTime().getDateTime();
		listGraphOut = "graph";
		startTS = 0;
		endTS = 2147483647;
		
		prettyJSON = false;
		numberOfContinuousTS = 0;
		buildingEdgeMode = ParsingConstants.DEFAULT_EDGE_MODE;
		numberOfIntervalTS = 0;
		percOfDeliveredMsg = 0;
	}

	public static void main(String[] args) {
		
		new LogParser();
		inputParameters(args);
		
		logger.info("Initializing...");
		
		try {
			if(fileInput != null && fileInput.exists()) {
				fis = new FileInputStream(fileInput);
			} else {
				if(fileInput != null && !fileInput.exists()) {
					System.out.println("ERROR: " + fileInput.getAbsolutePath()+ "- No such file or directory");
				}
				while(init()) {
					;
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		}
		
		GraphRepresentation graph = null;
		
		if(representationType == ParsingConstants.ADJACENCY_LISTS_REPRESENTATION) {
			graph = new AdjacencyListsModule(fileInput, fis);
			logger.info("Adjacency Lists Representation initialized.");
		}
		
		else
		
		if(representationType == ParsingConstants.ADJACENCY_MATRIX_REPRESENTATION) {
			graph = new AdjacencyMatrixModule(fileInput, fis);
			logger.info("Adjacency Matrix Representation initialized.");
		}
		
		else
			
		if(representationType == ParsingConstants.GEXF_REPRESENTATION) {
			System.out.println("\nWould you like to build...");
			System.out.println("1 - A dynamic graph (with information about time)");
			System.out.println("2 - An aggregate graph (SocialDIS Experiment)");
			System.out.println("3 - An aggregate graph (MACRO Experiment)");
			System.out.print("Please choose 1, 2 or 3: ");
			BufferedReader answer = new BufferedReader(new InputStreamReader(System.in));
			
			try {
				String choise = answer.readLine();
				while(!choise.equals("1") && !choise.equals("2") && !choise.equals("3")) {
					System.out.print("Please choose 1, 2 or 3: ");
					choise = answer.readLine();
				}
				
				if(choise.equals("1")) {
					System.out.print("Would you like to create virtual links? [y/N] ");
					if(answer.readLine().equalsIgnoreCase("y")) {
						graph = new GexfModuleDynamic(fileInput, fis, ParsingConstants.CREATE_VIRTUAL_LINKS);
						listGraphOut = listGraphOut + "_virtual_links";
					} else {
						graph = new GexfModuleDynamic(fileInput, fis, !ParsingConstants.CREATE_VIRTUAL_LINKS);
					}
					logger.info("Dynamic GEXF Representation initialized.");
				} else
				if(choise.equals("2")) {
					graph = new GexfModuleAggregateDIS(fileInput, fis);
					logger.info("Aggregate DIS GEXF Representation initialized.");
				} else
				if(choise.equals("3")) {
					boolean initReaders = false;
					System.out.print("Would you like to create reader relationships? [y/N] ");
					if(answer.readLine().equalsIgnoreCase("y")) {
						initReaders = true;
					}
					graph = new GexfModuleAggregateMacro(fileInput, fis, initReaders);
					logger.info("Aggregate MACRO GEXF Representation initialized.");
				}
				
			} catch (IOException e) {
				logger.error(e.getMessage());
				//e.printStackTrace();
			}
		}
		
		else
			
		if(representationType == ParsingConstants.JSON_REPRESENTATION) {
			
			try {
				System.out.print("Would you like to create virtual links? [y/N] ");
				BufferedReader answerVL = new BufferedReader(new InputStreamReader(System.in));
				String answerVlString = answerVL.readLine();
				
				boolean vl = false;
				if(answerVlString.equalsIgnoreCase("y") || answerVlString.equalsIgnoreCase("yes")) {
					vl = ParsingConstants.CREATE_VIRTUAL_LINKS;
					listGraphOut = listGraphOut + "_virtual_links";
				}
				
				System.out.println("\nWhat database would you like to use?");
				System.out.println("0. No one");
				System.out.println("1. SocialDIS");
				System.out.println("2. MACRO");
				System.out.print("Please choose 0, 1 or 2: ");
				BufferedReader answerDB = new BufferedReader(new InputStreamReader(System.in));
				
				String choise = answerDB.readLine();
				while(!choise.equals("0") && !choise.equals("1") && !choise.equals("2")) {
					System.out.print("Please choose 0, 1 or 2: ");
					choise = answerDB.readLine();
				}
				
				if(choise.equals("0")) {
					graph = new JsonModuleWithoutDB(fileInput, fis, vl);
				} else
				if(choise.equals("1")) {
					graph = new JsonModuleDIS(fileInput, fis, vl);
				} else
				if(choise.equals("2")) {
					graph = new JsonModuleMacro(fileInput, fis, vl);
				}
				
			} catch (IOException e) {
				logger.error("Wrong input.");
				//e.printStackTrace();
			}
			logger.info("JSON Representation initialized.");
		}
		
		else
			
		if(representationType == ParsingConstants.NETLOGO_REPRESENTATION) {
			System.out.println("\nWhat database would you like to use?");
			System.out.println("0. No one");
			System.out.println("1. MACRO");
			System.out.print("Please choose 0 or 1: ");
			BufferedReader answer = new BufferedReader(new InputStreamReader(System.in));
			
			try {
				String choise = answer.readLine();
				while(!choise.equals("0") && !choise.equals("1")) {
					System.out.print("Please choose 0 or 1: ");
					choise = answer.readLine();
				}
				
				System.out.print("Would you like to create virtual links? [y/N] ");
				boolean vl = false;
				if(answer.readLine().equalsIgnoreCase("y")) {
					vl = ParsingConstants.CREATE_VIRTUAL_LINKS;
					listGraphOut = listGraphOut + "_virtual_links";
				}
				
				System.out.print("Would you like to represent readers as nodes? [y/N] ");
				boolean initReaders = false;
				if(answer.readLine().equalsIgnoreCase("y")) {
					initReaders = true;
					listGraphOut = listGraphOut + "_with_readers";
				}
				
				if(choise.equals("0")) {
					graph = new NetLogoModule(fileInput, fis, vl, initReaders);
				} else
				if(choise.equals("1")) {
					graph = new NetLogoModuleMacro(fileInput, fis, vl, initReaders);
				}
				
			} catch (IOException e) {
				logger.error("Wrong input.");
				//e.printStackTrace();
			}
			logger.info("NetLogo Representation initialized.");
		}
		
		else
			
		if(representationType == ParsingConstants.DNF_REPRESENTATION) {
			try {
				System.out.print("Would you like to create virtual links? [y/N] ");
				BufferedReader answerVL = new BufferedReader(new InputStreamReader(System.in));
				String answerVlString = answerVL.readLine();
				
				boolean vl = false;
				if(answerVlString.equalsIgnoreCase("y") || answerVlString.equalsIgnoreCase("yes")) {
					vl = ParsingConstants.CREATE_VIRTUAL_LINKS;
					listGraphOut = listGraphOut + "_virtual_links";
				}
				
				System.out.println("\nWhat database would you like to use?");
				System.out.println("0. No one");
//				System.out.println("1. SocialDIS");
				System.out.println("2. MACRO");
//				System.out.print("Please choose 0, 1 or 2: ");
				System.out.print("Please choose 0 or 2: ");
				BufferedReader answerDB = new BufferedReader(new InputStreamReader(System.in));
				
				String choise = answerDB.readLine();
//				while(!choise.equals("0") && !choise.equals("1") && !choise.equals("2")) {
				while(!choise.equals("0") && !choise.equals("2")) {
//					System.out.print("Please choose 0, 1 or 2: ");
					System.out.print("Please choose 0 or 2: ");
					choise = answerDB.readLine();
				}
				
				if(choise.equals("0")) {
					graph = new DynamicNetworkModule(fileInput, fis, vl);
				} else
//				if(choise.equals("1")) {
//					graph = new JsonModuleDIS(fileInput, fis, vl);
//				} else
				if(choise.equals("2")) {
					graph = new DynamicNetworkModuleMacro(fileInput, fis, vl);
				}
				
			} catch (IOException e) {
				logger.error("Wrong input.");
				//e.printStackTrace();
			}
			logger.info("DNF Representation initialized.");
		}
		
		// Total file name
		listGraphOut = listGraphOut + fileType;
		
		try {
			graph.readLog(startTS, endTS);
			graph.writeFile(listGraphOut, separator);
		} catch (IOException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		}
		
		logger.info("Done. Good job!");
	}
	
	
	
	private static void inputParameters(String[] args) {
		boolean gexf = false;
		boolean json = false;
		boolean csv = false;
		boolean am = false;
		boolean net = false;
		boolean dnf = false;
		
		if(args.length > 0) {
			for(int i = 0; i < args.length; i++) {
				if(args[i].equals("-date") || args[i].equals("--datetime")) {
					listGraphOut = listGraphOut + "_" + dateTime[0] + "_" + dateTime[1];
				}
				
				else
					
				if(args[i].equals("-f") || args[i].equals("--file-name")) {
					if(args.length > i+1 && args[i+1].matches(".+")) {
						fileInput = new File(args[i+1]);
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-st") || args[i].equals("--start-timestamp")) {
					if(args.length > i+1 && args[i+1].matches("\\d+")) {
						startTS = Integer.parseInt(args[i+1]);
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-et") || args[i].equals("--end-timestamp")) {
					if(args.length > i+1 && args[i+1].matches("\\d+")) {
						endTS = Integer.parseInt(args[i+1]);
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-csv") || args[i].equals("--csv-format")) {
					if(!gexf && !json && !net && !dnf) {
						csv = true;
						separator = ParsingConstants.SEMICOLON_SEPARATOR;
						fileType = ParsingConstants.CSV_FILE_TYPE;
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-am") || args[i].equals("--adjacency-matrix")) {
					if(!gexf && !json && !net && !dnf) {
						am = true;
						representationType = ParsingConstants.ADJACENCY_MATRIX_REPRESENTATION;
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-gexf") || args[i].equals("--gexf")) {
					if(!csv && !am && !json && !net && !dnf) {
						gexf = true;
						separator = ParsingConstants.EMPTY_SEPARATOR;
						representationType = ParsingConstants.GEXF_REPRESENTATION;
						fileType = ParsingConstants.GEXF_FILE_TYPE;
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-json") || args[i].equals("--json")) {
					if(!csv && !am && !gexf && !net && !dnf) {
						json = true;
						separator = ParsingConstants.EMPTY_SEPARATOR;
						representationType = ParsingConstants.JSON_REPRESENTATION;
						fileType = ParsingConstants.JSON_FILE_TYPE;
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-net") || args[i].equals("--netlogo")) {
					if(!csv && !am && !json && !gexf && !dnf) {
						net = true;
						separator = ParsingConstants.EMPTY_SEPARATOR;
						representationType = ParsingConstants.NETLOGO_REPRESENTATION;
						fileType = ParsingConstants.TXT_FILE_TYPE;
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-dnf") || args[i].equals("--dynamic-network-format")) {
					if(!csv && !am && !json && !gexf && !net) {
						dnf = true;
						separator = ParsingConstants.EMPTY_SEPARATOR;
						representationType = ParsingConstants.DNF_REPRESENTATION;
						fileType = ParsingConstants.DNF_FILE_TYPE;
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-pp") || args[i].equals("--pretty-printing")) {
					if(!csv && !am && !gexf && !net && !dnf) {
						LogParser.prettyJSON = true;
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-ct") || args[i].equals("--continuous-timestamps")) {
					
					if(LogParser.buildingEdgeMode != ParsingConstants.DEFAULT_EDGE_MODE) {
						System.out.println("\nERROR:\nThe -ct or --continuous-timestamps option is incompatible with the \"interval timestamps\" mode!\n");
						System.exit(0);
					}
					
					if(args.length > i+1 && args[i+1].matches("\\d+")) {
						LogParser.numberOfContinuousTS = Integer.parseInt(args[i+1]);
						if(LogParser.numberOfContinuousTS > 1) {
							LogParser.buildingEdgeMode = ParsingConstants.CONTINUOUS_EDGE_MODE;
						} else {
							System.out.println("\nERROR:\nThe number of continuous timestamps must be greater than 1!\n");
							System.exit(0);
						}
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-it") || args[i].equals("--interval-timestamps")) {
					
					if(LogParser.buildingEdgeMode != ParsingConstants.DEFAULT_EDGE_MODE) {
						System.out.println("\nERROR:\nThe -it or --interval-timestamps option is incompatible with the \"continuous timestamps\" mode.\n");
						System.exit(0);
					}
					
					if(args.length > i+2 && args[i+1].matches("\\d+") && args[i+2].matches("\\d+")) {					
						LogParser.numberOfIntervalTS = Integer.parseInt(args[i+1]);
						LogParser.percOfDeliveredMsg = Integer.parseInt(args[i+2]);
						if(LogParser.numberOfIntervalTS > 1 && LogParser.percOfDeliveredMsg > 1 && LogParser.percOfDeliveredMsg <= 100) {
							LogParser.buildingEdgeMode = ParsingConstants.INTERVAL_EDGE_MODE;
						} else {
							System.out.println("\nERROR:\nThe number of the timestamp interval must be greater than 1.\nThe percentage must be in the range [1,100].\n");
							System.exit(0);
						}
					} else {
						usage();
						System.exit(0);
					}
				}
				
				else
					
				if(args[i].equals("-h") || args[i].equals("--help")) {
					usage();
					System.exit(0);
				}
				
				else
					
				if(args[i].matches(".+")) {
					;
				}
				
				else {
					usage();
					System.exit(0);
				}
			}
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
		} else {
			System.out.println("ERROR: " + fileInput.getAbsolutePath()+ "- No such file or directory");
		}
		
		return missingfile;
	}
	
	
	private static void usage() {
		System.out.println("\nTARGET SPECIFICATION:");
		System.out.println("-h or --help: Getting this help.");
		System.out.println("-date or --datetime: Append date and time to the file name.");
		System.out.println("-csv or --csv-format: The output file type will be in CSV format (only for Adjacency Lists and Adjacency Matrix).");
		System.out.println("-am or --adjacency-matrix: The output graph will be represented by adjacency matrix.");
		System.out.println("-gexf or --gexf: The output graph will be represented by XML Gephi format. This is incompatible with other representation parameters.");
		System.out.println("-json or --json: The output graph will be represented by JSON format. This is incompatible with other representation parameters.");
		System.out.println("-net or --netlogo: The output graph will be represented for the NetLogo processing. This is incompatible with other representation parameters.");
		System.out.println("-dnf or --dynamic-network-format: The output graph will be represented by DNF format. This is incompatible with other representation parameters.");
		System.out.println("-st <INTVALUE> or --start-timestamp <INTVALUE>: The initial timestamp.");
		System.out.println("-et <INTVALUE> or --end-timestamp <INTVALUE>: The final timestamp.");
		System.out.println("-pp or --pretty-printing: use this option with -json option if you'd like to enable the JSON pretty printing.");
		System.out.println("-ct <INTVALUE> or --continuous-timestamps <INTVALUE>: specify the number of continuous timestamps to build an edge.");
		System.out.println("-it <INTVALUE> <INTVALUE> or --interval-timestamps <INTVALUE> <INTVALUE>: specify the timestamp interval and the percentage of messages to build an edge.\n");
	}	
}
