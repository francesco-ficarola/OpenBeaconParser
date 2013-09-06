package it.uniroma1.dis.wsngroup.parsing.modules.dis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import it.uniroma1.dis.wsngroup.gexf4j.core.Edge;
import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.constants.DBConstants;
import it.uniroma1.dis.wsngroup.constants.ParsingConstants;
import it.uniroma1.dis.wsngroup.db.DBObject;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

/**
 * @author Francesco Ficarola
 *
 */

public class GexfModuleAggregateDIS implements GraphRepresentation {

	private Logger logger = Logger.getLogger(this.getClass());
	
	private File fileInput;
	private FileInputStream fis;
	private ArrayList<Tag> tagsDB;
	private Gexf xmlGraph;
	private Attribute attAge;
	private Attribute attGender;
	private Attribute attCourse;
	private Attribute attYear;
	private AttributeList nodeAttrList;
	private int indexEdges;
	private Node sourceNode;
	private Node targetNode;
	private boolean firstLogRow;
	private Integer startRealTS;
	private Integer stopRealTS;
	
	
	public GexfModuleAggregateDIS(File fileInput, FileInputStream fis) {
		this.fileInput = fileInput;
		this.fis = fis;
		
		// Lettura DB e memorizzazione in un ArrayList di Tag
		tagsDB = readDB();
		
		// Creazione grafo
		xmlGraph = new GexfImpl();
		xmlGraph.getMetadata().setLastModified(new Date()).setCreator("OpenBeacon Parser").setDescription("The SocialDIS Experiment");
		xmlGraph.getGraph().setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);
		
		// Creazione lista attributi per i nodi
		nodeAttrList = new AttributeListImpl(AttributeClass.NODE);
		xmlGraph.getGraph().getAttributeLists().add(nodeAttrList);
		attGender = nodeAttrList.createAttribute("0", AttributeType.STRING, "gender");
		attAge = nodeAttrList.createAttribute("1", AttributeType.INTEGER, "age");
		attCourse = nodeAttrList.createAttribute("2", AttributeType.STRING, "course");
		attYear = nodeAttrList.createAttribute("3", AttributeType.INTEGER, "year");
		
		indexEdges = 0;
		
		firstLogRow = false;
		startRealTS = 0;
		stopRealTS = 0;
	}
	
	
	private void packetParsing(String s, Integer startTS, Integer endTS) throws IOException {
		logger.debug(s);
		
		String currentToken = "";
		ArrayList<String> tokens = new ArrayList<String>();
		
		boolean addToken = false;
		StringTokenizer st = new StringTokenizer(s, " ");
		
		while(st.hasMoreTokens()) {
			currentToken = st.nextToken();
						
			if(currentToken.matches("\\bS\\b") || addToken) {
				addToken = true;
				tokens.add(currentToken);
			}
			
			else
				
			if(currentToken.matches("\\bC\\b") || addToken) {
				addToken = true;
				tokens.add(currentToken);
			}
		}
		
		// Vincolo sul timestamp
		if(tokens.size() > 1 && (Integer.parseInt(tokens.get(ParsingConstants.TIMESTAMP_INDEX).replace(ParsingConstants.TIMESTAMP_PREFIX, "")) >= startTS &&
				Integer.parseInt(tokens.get(ParsingConstants.TIMESTAMP_INDEX).replace(ParsingConstants.TIMESTAMP_PREFIX, "")) <= endTS)) {
			
			// Memorizzazione del primo e dell'ultimo TS reali
			// (serviranno per calcolare la normalizzazione)
			if(!firstLogRow) {
				startRealTS = Integer.parseInt(tokens.get(ParsingConstants.TIMESTAMP_INDEX).replace(ParsingConstants.TIMESTAMP_PREFIX, ""));
				firstLogRow = true;
			} else {
				stopRealTS = Integer.parseInt(tokens.get(ParsingConstants.TIMESTAMP_INDEX).replace(ParsingConstants.TIMESTAMP_PREFIX, ""));
			}
			
			
			String idSourceNode = "";
			String idTargetNode = "";
			
			for(int i = 0; i < tokens.size(); i++) {
				
				/* 
				 * RILEVAMENTI
				 */				
				if(tokens.get(ParsingConstants.TYPE_MESSAGE_INDEX).equals("S")) {
					if(tokens.get(i).contains(ParsingConstants.SOURCE_PREFIX)) {
						List<Node> listNode = xmlGraph.getGraph().getNodes();
						idSourceNode = tokens.get(i).replace(ParsingConstants.SOURCE_PREFIX, "");
						Integer indexSearchedNode = searchIndexNode(listNode, idSourceNode);
						
						// Se non esiste un nodo con id  uguale a
						// idSourceNode allora provvedo a crearlo.
						if(indexSearchedNode.equals(-1)) {
							// Se esiste una corrispondenza tra idSourceNode ed un tag
							// presente nel DB avente medesimo ID, allora creo il nodo.
							int indexNodeDB = searchTagInDB(Integer.parseInt(idSourceNode));
							if(indexNodeDB >= 0) {
								Node node = xmlGraph.getGraph().createNode(idSourceNode);
								node.setLabel(idSourceNode);
								node.getAttributeValues().
									addValue(attGender, tagsDB.get(indexNodeDB).getGender()).
									addValue(attAge, String.valueOf(tagsDB.get(indexNodeDB).getAge())).
									addValue(attCourse, tagsDB.get(indexNodeDB).getCourse()).
									addValue(attYear, String.valueOf(tagsDB.get(indexNodeDB).getYear()));
							} else {
								logger.error("No match for the tag " + idSourceNode);
							}
						}
					}
				}
				
				else
				
				if(tokens.get(ParsingConstants.TYPE_MESSAGE_INDEX).equals("C")) {
					
					if(tokens.get(i).contains(ParsingConstants.SOURCE_PREFIX)) {						
						List<Node> listNode = xmlGraph.getGraph().getNodes();
						idSourceNode = tokens.get(i).replace(ParsingConstants.SOURCE_PREFIX, "");
						Integer indexSearchedNode = searchIndexNode(listNode, idSourceNode);
						
						// Se non esiste un nodo con id  uguale a
						// idSourceNode allora provvedo a crearlo.
						if(indexSearchedNode.equals(-1)) {
							// Se esiste una corrispondenza tra idSourceNode ed un tag
							// presente nel DB avente medesimo ID, allora creo il nodo.
							int indexNodeDB = searchTagInDB(Integer.parseInt(idSourceNode));
							if(indexNodeDB >= 0) {
								sourceNode = xmlGraph.getGraph().createNode(idSourceNode);
								sourceNode.setLabel(idSourceNode);
								sourceNode.getAttributeValues().
									addValue(attGender, tagsDB.get(indexNodeDB).getGender()).
									addValue(attAge, String.valueOf(tagsDB.get(indexNodeDB).getAge())).
									addValue(attCourse, tagsDB.get(indexNodeDB).getCourse()).
									addValue(attYear, String.valueOf(tagsDB.get(indexNodeDB).getYear()));
							} else {
								logger.error("No match for the tag " + idSourceNode);
							}
						} else {
							// Se esiste già un nodo con id uguale a idSourceNode allora
							// lo recupero per permettere la creazione dell'arco più avanti.
							sourceNode = xmlGraph.getGraph().getNodes().get(indexSearchedNode.intValue());
						}
					}
					
					else
					
					
					/*
					 * CONTATTI DIRETTI
					 */
					
					// Espressione regolare per un modello simile a [1203(1) :
					// \\x5b = [
					// \\d+ = qualsiasi numero ripetuto da 1 a n volte
					// .* = qualsiasi carattere ripetuto da 0 a n volte
					// \\x28 = (
					// \\x29 = )
					if(tokens.get(i).matches("\\x5b\\d+.*")) {					
						List<Node> listNodes = xmlGraph.getGraph().getNodes();
						List<Edge> listEdges = xmlGraph.getGraph().getAllEdges();
						idTargetNode = tokens.get(i).replaceAll("\\x5b|\\x28\\d+\\x29", "");
						Integer indexSearchedNode = searchIndexNode(listNodes, idTargetNode);
						
						// Se non esiste un nodo con id  uguale a
						// idTargetNode allora provvedo a crearlo.
						if(indexSearchedNode.equals(-1)) {
							// Se esiste una corrispondenza tra idTargetNode ed un tag
							// presente nel DB avente medesimo ID, allora creo il nodo.
							int indexNodeDB = searchTagInDB(Integer.parseInt(idTargetNode));
							if(indexNodeDB >= 0) {
								targetNode = xmlGraph.getGraph().createNode(idTargetNode);
								targetNode.setLabel(idTargetNode);
								targetNode.getAttributeValues().
									addValue(attGender, tagsDB.get(indexNodeDB).getGender()).
									addValue(attAge, String.valueOf(tagsDB.get(indexNodeDB).getAge())).
									addValue(attCourse, tagsDB.get(indexNodeDB).getCourse()).
									addValue(attYear, String.valueOf(tagsDB.get(indexNodeDB).getYear()));
							} else {
								logger.error("No match for the tag " + idTargetNode);
							}
						} else {
							// Se esiste già un nodo con id uguale a idTargetNode allora
							// lo recupero per permettere la creazione dell'arco più avanti.
							targetNode = xmlGraph.getGraph().getNodes().get(indexSearchedNode.intValue());
						}
						
						// Creazione arco tra sourceNode e targetNode
						if(!sourceNode.hasEdgeTo(idTargetNode) && !targetNode.hasEdgeTo(idSourceNode)) {
							sourceNode.connectTo(String.valueOf(indexEdges), targetNode).setWeight(1);
							indexEdges++;
						} else {
							Integer indexExistEdge = searchIndexEdge(listEdges, sourceNode, targetNode);
							if(!indexExistEdge.equals(-1)) {
								setWeightExistingEdges(indexExistEdge);
							}
						}
					}
				}
			}
		}
	}
	
	
	private void weightNormalizing() {
		float totalTime = stopRealTS - startRealTS;
		List<Edge> edgeList = xmlGraph.getGraph().getAllEdges();
		for(int i = 0; i < edgeList.size(); i++) {
			edgeList.get(i).setWeight(edgeList.get(i).getWeight() * 1000 / totalTime);
		}
	}
	
	
	private void setWeightExistingEdges(Integer indexExistEdge) {
		List<it.uniroma1.dis.wsngroup.gexf4j.core.Edge> edgeList = xmlGraph.getGraph().getAllEdges();	
		edgeList.get(indexExistEdge).setWeight(edgeList.get(indexExistEdge).getWeight() + 1);
	}
	
	
	private Integer searchIndexEdge(List<it.uniroma1.dis.wsngroup.gexf4j.core.Edge> listEdges, Node a, Node b) {
		Integer indexExistEdge = -1;
		if(listEdges != null) {
			for(int i = 0; i < listEdges.size(); i++) {
				if((listEdges.get(i).getSource() == a && listEdges.get(i).getTarget() == b) ||
					(listEdges.get(i).getSource() == b && listEdges.get(i).getTarget() == a)) {
					indexExistEdge = i;
					break;
				}
			}
		}
		return indexExistEdge;
	}
	
	
	private Integer searchIndexNode(List<Node> listNodes, String currentId) {
		Integer indexExistNode = -1;
		
		if(listNodes != null && listNodes.size() > 0) {
			for(int i = 0; i < listNodes.size(); i++) {
				if(listNodes.get(i).getId().equals(currentId)) {
					indexExistNode = i;
					break;
				}
			}
		}
		return indexExistNode;
		
	}
	
	
	private int searchTagInDB(Integer id) {		
		Comparator<Tag> comparator = new Comparator<Tag>() {
			public int compare(Tag t1, Tag t2) {
				return Integer.valueOf(t1.getTagID()).compareTo(Integer.valueOf(t2.getTagID()));
			}
		};
		
		return Collections.binarySearch(tagsDB, new Tag(Integer.valueOf(id)), comparator);
	}
	
	
	private ArrayList<Tag> readDB() {
		ArrayList<Tag> tagsDB = new ArrayList<Tag>();
		
		DBObject db = new DBObject(DBConstants.DB_TYPE, DBConstants.DB_SERVER_HOSTNAME, DBConstants.DB_SERVER_PORT, DBConstants.DIS_DB_NAME, DBConstants.DIS_DB_USERNAME, DBConstants.DIS_DB_PASSWORD);
		Connection connection = null;
		try {
			
			connection = db.getConnection();			
			logger.info("Connected to database " + DBConstants.DIS_DB_NAME + " @ " + DBConstants.DB_SERVER_HOSTNAME);
			
			Statement statement = connection.createStatement();
			String querySelect = "SELECT * FROM " + DBConstants.DIS_TAG_TABLE_NAME;
			ResultSet rsSelect = statement.executeQuery(querySelect);
			
			while(rsSelect.next()) {
				Tag tag = new Tag(
						rsSelect.getInt("TagID"),
						rsSelect.getString("Gender"),
						rsSelect.getInt("Age"),
						rsSelect.getString("Course"),
						rsSelect.getInt("Year")
						);
				
				tagsDB.add(tag);
			}
			
			statement.close();
			Collections.sort(tagsDB);
			
			for(int i = 0; i < tagsDB.size(); i++) {
				logger.debug(tagsDB.get(i).toString());
			}
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		} finally {
			if (connection != null) {
		    	  try {
					connection.close();
					logger.info("Disconnected to database " + DBConstants.DIS_DB_NAME + " @ " + DBConstants.DB_SERVER_HOSTNAME);
				} catch (SQLException e) {
					logger.error(e.getMessage());
					//e.printStackTrace();
				}
			}
	    }
		
		return tagsDB;
	}

	
	public void readLog(Integer startTS, Integer endTS) throws IOException {
		logger.info("Reading...");
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fis));
			
			String currentLine;
			while((currentLine=br.readLine()) != null) {
				if(!currentLine.trim().equalsIgnoreCase("")) {
					logger.debug(currentLine.trim());
					packetParsing(currentLine.trim(), startTS, endTS);
				}
			}
		} finally {
			if(br != null) {
				br.close();
			}
		}
	}

	
	public void writeFile(String outputFileName, String separator) throws IOException {
		logger.info("Writing...");
		
		weightNormalizing();
		
		StaxGraphWriter graphWriter = new StaxGraphWriter();
		File f = new File(fileInput.getParentFile() + "/" + outputFileName);
		FileOutputStream fos = new FileOutputStream(f, false);
		
		graphWriter.writeToStream(xmlGraph, fos, "UTF-8");
		
		logger.info("Output file: " + f.getAbsolutePath());
	}
	
	/*
	 * INNER CLASS
	 */
	private class Tag implements Comparable<Tag> {

		private int tagID;
		private String gender;
		private int age;
		private String course;
		private int year;
		
		public Tag(int tagID, String gender, int age, String course, int year) {
			this.tagID = tagID;
			this.gender = gender;
			this.age = age;
			this.course = course;
			this.year = year;
		}
		
		public Tag(int tagID) {
			this.tagID = tagID;
		}
		
		public int getTagID() {
			return tagID;
		}
		
		public String getGender() {
			return gender;
		}

		public int getAge() {
			return age;
		}

		public String getCourse() {
			return course;
		}

		public int getYear() {
			return year;
		}

		public int compareTo(Tag t) {
			return Integer.valueOf(tagID).compareTo(t.tagID);
		}
		
		@Override
		public String toString() {
			return "Tag [tagID=" + tagID + ", gender=" + gender + ", age="
					+ age + ", course=" + course + ", year=" + year + "]";
		}
	}

}
