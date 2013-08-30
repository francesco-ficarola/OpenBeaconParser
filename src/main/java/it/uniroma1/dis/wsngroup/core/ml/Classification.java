package it.uniroma1.dis.wsngroup.core.ml;

import it.uniroma1.dis.wsngroup.constants.ParsingConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * @author Francesco Ficarola
 */

public class Classification {
	
	private static Logger logger = Logger.getLogger(Classification.class);
	
	private static boolean genDatasets;
	private static boolean outcomes;
	private static File fileJson;
	private static FileInputStream fis;
	private static String separator;
	private static List<TimestampObject> tsObjectList;
	private static File path;
	
	public Classification() {
		genDatasets = false;
		outcomes = true;
		separator = ParsingConstants.COMMA_SEPARATOR;
		path = new File("../ml/");
	}
	
	public static void main(String[] args) {
		
		new Classification();
		inputParameters(args);
		
		logger.info("Initializing...");
		
		if(genDatasets) {
			try {
				while(init()) {
					;
				}
			} catch (IOException e) {
				logger.error(e.getMessage());
				//e.printStackTrace();
			}
			
			
			logger.info("Importing the JSON file...");
			ImportJsonData inputFile = new ImportJsonData(fis);
			tsObjectList = inputFile.readJson();
			
			
			if(tsObjectList != null) {
				logger.debug("Total timestamps: " + tsObjectList.size());
				logger.info("Building Datasets...");
				MacroDataset mds = new MacroDataset(separator, fileJson.getParentFile());
				mds.buildDataset(tsObjectList);
			} else {
				logger.error("tsObjectList is null!");
				return;
			}
		}
		
		logger.info("Importing and elaborating the datasets...");
		NaiveBayesClassificator nbc = new NaiveBayesClassificator(path, outcomes);
		nbc.importDatasets();	
	}

	private static boolean init() throws IOException {
		boolean missingfile = true;
		
		System.out.print("JSON to import: ");
		BufferedReader key = new BufferedReader(new InputStreamReader(System.in));

		fileJson = new File(key.readLine());
		path = fileJson.getParentFile();
		
		if(fileJson.exists()) {
			missingfile = false;
			fis = new FileInputStream(fileJson);
		} else {
			System.out.println("ERROR: " + fileJson.getAbsolutePath()+ "- No such file or directory");
		}
		
		return missingfile;
	}
	
	
	private static void inputParameters(String[] args) {
		if(args.length > 0) {
			for(int i = 0; i < args.length; i++) {
				if(args[i].equals("-gendata") || args[i].equals("--generate-datasets")) {
					genDatasets = true;
				}
				
				else
					
				if(args[i].equals("-noout") || args[i].equals("--no-outcomes")) {
					outcomes = false;
				}
				
				else
				
				if(args[i].equals("-h") || args[i].equals("--help")) {
					usage();
					System.exit(0);
				}
				
				else {
					usage();
					System.exit(0);
				}
			}
		}
	}
	
	
	private static void usage() {
		System.out.println("\nTARGET SPECIFICATION:");
		System.out.println("-h or --help: Getting this help.");
		System.out.println("-gendata or --generate-datasets: import an existing JSON file and build datasets.");
		System.out.println("-noout or --no-outcomes: No outcome will be exported in CSV format.\n");
	}	
	
	
	/*
	 * INNER CLASS
	 */
	protected class TimestampObject {
		private int id;
		private String timestamp;
		private ArrayList<Visitor> visitors;
		private ArrayList<Seq> seqNumbers;
		private ArrayList<Reader> readers;
		private ArrayList<Edge> edges;

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(String timestamp) {
			this.timestamp = timestamp;
		}
		
		public ArrayList<Visitor> getVisitors() {
			return visitors;
		}

		public void setVisitors(ArrayList<Visitor> visitors) {
			this.visitors = visitors;
		}

		public ArrayList<Seq> getSeqNumbers() {
			return seqNumbers;
		}

		public void setSeqNumbers(ArrayList<Seq> seqNumbers) {
			this.seqNumbers = seqNumbers;
		}

		public ArrayList<Reader> getReaders() {
			return readers;
		}

		public void setReaders(ArrayList<Reader> readers) {
			this.readers = readers;
		}
		
		public ArrayList<Edge> getEdges() {
			return edges;
		}

		public void setEdges(ArrayList<Edge> edges) {
			this.edges = edges;
		}
	}
	
	
	protected class Visitor implements Comparable<Visitor> {
		private int personid;
		private int badgeid;
		private String sex;
		private int age;
		private String grade;
		private String occupation;
		private String nationality;
		private boolean guide;
		private int groupid;
		private Integer startdate;
		private Integer enddate;
		private ArrayList<String> readerIDs;
		
		public Visitor(int personid, int badgeid, String sex, int age, String grade, String occupation,
				String nationality, boolean guide, int groupid, Integer startdate, Integer enddate, ArrayList<String> readerIDs) {
			this.personid = personid;
			this.badgeid = badgeid;
			this.sex = sex;
			this.age = age;
			this.grade = grade;
			this.occupation = occupation;
			this.nationality = nationality;
			this.guide = guide;
			this.groupid = groupid;
			this.startdate = startdate;
			this.enddate = enddate;
			this.readerIDs = readerIDs;
		}
		
		public Visitor(int tagID) {
			this.personid = tagID;
		}
		
		public int getPersonid() {
			return personid;
		}
		
		public int getBadgeid() {
			return badgeid;
		}
		
		public String getSex() {
			return sex;
		}

		public int getAge() {
			return age;
		}

		public String getGrade() {
			return grade;
		}

		public String getOccupation() {
			return occupation;
		}

		public String getNationality() {
			return nationality;
		}

		public boolean isGuide() {
			return guide;
		}

		public int getGroupid() {
			return groupid;
		}

		public Integer getStartdate() {
			return startdate;
		}

		public Integer getEnddate() {
			return enddate;
		}

		public ArrayList<String> getReaderIDs() {
			return readerIDs;
		}


		public void setReaderIDs(ArrayList<String> readerIDs) {
			this.readerIDs = readerIDs;
		}

		public int compareTo(Visitor t) {
			return Integer.valueOf(personid).compareTo(t.personid);
		}

		@Override
		public String toString() {
			return "Visitor [personid=" + personid + ", badgeid=" + badgeid
					+ ", sex=" + sex + ", age=" + age + ", grade=" + grade
					+ ", occupation=" + occupation + ", nationality="
					+ nationality + ", guide=" + guide + ", groupid=" + groupid
					+ ", startdate=" + startdate + ", enddate=" + enddate
					+ ", readerIDs=" + readerIDs + "]";
		}
	}
	
	
	private class Seq {
//		private String seqNumber;
//		private int tagID;
//		
//		public Seq(String seqNumber, int tagID) {
//			this.seqNumber = seqNumber;
//			this.tagID = tagID;
//		}
//		
//		public String getSeqNumber() {
//			return seqNumber;
//		}
//		
//		public int getTagID() {
//			return tagID;
//		}
	}
	
	
	private class Reader {
//		private String readerID;
//		private String room;
//		
//		public Reader(String readerID, String room) {
//			this.readerID = readerID;
//		}
//		
//		public String getReaderID() {
//			return readerID;
//		}
//
//		public String getRoom() {
//			return room;
//		}
	}
	
	
	protected class Edge {
		private ArrayList<Integer> source_target;
		private int power;
		
		public Edge(ArrayList<Integer> source_target, int power) {
			this.source_target = source_target;
			this.power = power;
		}

		public ArrayList<Integer> getSourceTarget() {
			return source_target;
		}

		public int getPower() {
			return power;
		}
	}
}
