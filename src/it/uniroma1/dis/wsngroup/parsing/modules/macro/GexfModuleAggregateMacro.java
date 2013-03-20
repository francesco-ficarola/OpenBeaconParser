package it.uniroma1.dis.wsngroup.parsing.modules.macro;

import it.uniroma1.dis.wiserver.gexf4j.core.Edge;
import it.uniroma1.dis.wiserver.gexf4j.core.EdgeType;
import it.uniroma1.dis.wiserver.gexf4j.core.Gexf;
import it.uniroma1.dis.wiserver.gexf4j.core.Graph;
import it.uniroma1.dis.wiserver.gexf4j.core.Mode;
import it.uniroma1.dis.wiserver.gexf4j.core.Node;
import it.uniroma1.dis.wiserver.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wiserver.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wiserver.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wiserver.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wiserver.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wiserver.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wiserver.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.constants.DBConstants;
import it.uniroma1.dis.wsngroup.db.DBObject;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;


public class GexfModuleAggregateMacro implements GraphRepresentation {
	
	/**
	 * @author Francesco Ficarola
	 *
	 */
	
	private Logger logger = Logger.getLogger(this.getClass());
	
	private File fileInput;
	private FileInputStream fis;
	private ArrayList<Visitor> usersDB;
	private Gexf xmlGraph;
	private Graph graph;
	private Attribute attBadgeid;
	private Attribute attSex;
	private Attribute attAge;
	private Attribute attGrade;
	private Attribute attOccupation;
	private Attribute attNationality;
	private Attribute attGuide;
	private Attribute attGroup;
	private AttributeList nodeAttrList;
	private int indexEdges;
	private Node sourceNode;
	private Node targetNode;
	private boolean firstLogRow;
	private Integer startRealTS;
	private Integer stopRealTS;
	private boolean initReaders;
	private Map<String,Node> readers;
	
	public GexfModuleAggregateMacro(File fileInput, FileInputStream fis, boolean initReaders) {
		this.fileInput = fileInput;
		this.fis = fis;
		
		// Lettura DB e memorizzazione in un ArrayList di Tag
		usersDB = readDB();
		
		// Creazione grafo
		xmlGraph = new GexfImpl();
		xmlGraph.getMetadata().setLastModified(new Date()).setCreator("Francesco Ficarola").setDescription("MACRO NEON 2012");
		
		graph = xmlGraph.getGraph();
		graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);
		
		// Creazione lista attributi per i nodi
		nodeAttrList = new AttributeListImpl(AttributeClass.NODE);
		graph.getAttributeLists().add(nodeAttrList);
		attBadgeid = nodeAttrList.createAttribute("0", AttributeType.INTEGER, "badgeid");
		attSex = nodeAttrList.createAttribute("1", AttributeType.STRING, "sex");
		attAge = nodeAttrList.createAttribute("2", AttributeType.INTEGER, "age");
		attGrade = nodeAttrList.createAttribute("3", AttributeType.STRING, "grade");
		attOccupation = nodeAttrList.createAttribute("4", AttributeType.STRING, "occupation");
		attNationality = nodeAttrList.createAttribute("5", AttributeType.STRING, "nationality");
		attGuide = nodeAttrList.createAttribute("6", AttributeType.BOOLEAN, "guide");
		attGroup = nodeAttrList.createAttribute("7", AttributeType.INTEGER, "groupid");
		
		indexEdges = 0;
		
		firstLogRow = false;
		startRealTS = 0;
		stopRealTS = 0;
		
		this.initReaders = initReaders;
		if(this.initReaders) {
			readers = new HashMap<String, Node>();
			initReaders();
		}
	}
	
	
	private void initReaders() {
		Node reader1 = graph.createNode("0xc0a85016").setLabel("0xc0a85016");
		readers.put("0xc0a85016", reader1);
		
		Node reader2 = graph.createNode("0xc0a85009").setLabel("0xc0a85009");
		readers.put("0xc0a85009", reader2);
		
		Node reader3 = graph.createNode("0xc0a8500d").setLabel("0xc0a8500d");
		readers.put("0xc0a8500d", reader3);
		
		Node reader4 = graph.createNode("0xc0a85013").setLabel("0xc0a85013");
		readers.put("0xc0a85013", reader4);
		
		Node reader5 = graph.createNode("0xc0a8501a").setLabel("0xc0a8501a");
		readers.put("0xc0a8501a", reader5);
		
		Node reader6 = graph.createNode("0xc0a85017").setLabel("0xc0a85017");
		readers.put("0xc0a85017", reader6);
		
		Node reader7 = graph.createNode("0xc0a85019").setLabel("0xc0a85019");
		readers.put("0xc0a85019", reader7);
		
		Node reader8 = graph.createNode("0xc0a85012").setLabel("0xc0a85012");
		readers.put("0xc0a85012", reader8);
	}
	
	
	private void createEdgeNodeAndReader(Node node, String readerIp) {
		Node reader = readers.get(readerIp);
		if(!node.hasEdgeTo(readerIp) && !reader.hasEdgeTo(node.getId())) {
			reader.connectTo(String.valueOf(indexEdges), node).setWeight(1);
			indexEdges++;
		} else {
			Integer indexExistEdge = searchIndexEdge(graph.getAllEdges(), node, reader);
			if(!indexExistEdge.equals(-1)) {
				setWeightExistingEdges(indexExistEdge);
			}
		}
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
		
		if(tokens.size() > 1) {
			
			Integer currentTimestamp = Integer.parseInt(tokens.get(1).replace("t=", ""));
		
			// Vincolo sul timestamp
			if(currentTimestamp >= startTS && currentTimestamp <= endTS) {
				
				// Memorizzazione del primo e dell'ultimo TS reali
				// (serviranno per calcolare la normalizzazione)
				if(!firstLogRow) {
					startRealTS = Integer.parseInt(tokens.get(1).replace("t=", ""));
					firstLogRow = true;
				} else {
					stopRealTS = Integer.parseInt(tokens.get(1).replace("t=", ""));
				}
				
				
				Integer idPersonSourceNode = -1;
				Integer idPersonTargetNode = -1;
				
				for(int i = 0; i < tokens.size(); i++) {
					
					/* 
					 * RILEVAMENTI
					 */				
					if(tokens.get(0).equals("S")) {
						if(tokens.get(i).contains("id=")) {
							List<Node> listNodes = graph.getNodes();
							String idBadgeSourceNode = tokens.get(i).replace("id=", "");
							
							int indexNodeDB = searchVisitorInDB(Integer.parseInt(idBadgeSourceNode), currentTimestamp);
							
							// Se esiste una corrispondenza tra idBadgeSourceNode ed un visitatore
							// presente nel DB avente medesimo ID, allora creo il nodo.
							if(indexNodeDB >= 0) {
								
								Visitor visitor = usersDB.get(indexNodeDB);
								idPersonSourceNode = visitor.getPersonid();
								
								Integer indexSearchedNode = searchIndexNode(listNodes, idPersonSourceNode);
								
								// Se non esiste un nodo nel grafo con id  uguale a
								// idPersonSourceNode allora provvedo a crearlo.
								if(indexSearchedNode.equals(-1)) {
									Node node = graph.createNode(String.valueOf(idPersonSourceNode));
									node.setLabel(String.valueOf(idPersonSourceNode));
									node.getAttributeValues().
										addValue(attBadgeid, String.valueOf(visitor.getBadgeid())).
										addValue(attSex, visitor.getSex()).
										addValue(attAge, String.valueOf(visitor.getAge())).
										addValue(attGrade, visitor.getGrade()).
										addValue(attOccupation, visitor.getOccupation()).
										addValue(attNationality, visitor.getNationality()).
										addValue(attGuide, String.valueOf(visitor.isGuide())).
										addValue(attGroup, String.valueOf(visitor.getGroupid()));
									
									if(initReaders) {
										String reader = tokens.get(2).replace("ip=", "");
										createEdgeNodeAndReader(node, reader);
									}
								} else {
									if(initReaders) {
										Node node = listNodes.get(indexSearchedNode.intValue());
										String reader = tokens.get(2).replace("ip=", "");
										createEdgeNodeAndReader(node, reader);
									}
								}
								
							} else {
								logger.error("No match for the tag " + idBadgeSourceNode + " for TS " + currentTimestamp);
							}
						}
					}
					
					else
					
					if(tokens.get(0).equals("C")) {
						
						if(tokens.get(i).contains("id=")) {						
							List<Node> listNodes = graph.getNodes();
							String idBadgeSourceNode = tokens.get(i).replace("id=", "");
							
							int indexNodeDB = searchVisitorInDB(Integer.parseInt(idBadgeSourceNode), currentTimestamp);
							
							// Se esiste una corrispondenza tra idBadgeSourceNode ed un visitatore
							// presente nel DB avente medesimo ID, allora creo il nodo.
							if(indexNodeDB >= 0) {
	
								Visitor visitor = usersDB.get(indexNodeDB);
								idPersonSourceNode = visitor.getPersonid();
								
								Integer indexSearchedNode = searchIndexNode(listNodes, idPersonSourceNode);
								
								// Se non esiste un nodo nel grafo con id  uguale a
								// idPersonSourceNode allora provvedo a crearlo.
								if(indexSearchedNode.equals(-1)) {
									sourceNode = graph.createNode(String.valueOf(idPersonSourceNode));
									sourceNode.setLabel(String.valueOf(idPersonSourceNode));
									sourceNode.getAttributeValues().
										addValue(attBadgeid, String.valueOf(visitor.getBadgeid())).
										addValue(attSex, visitor.getSex()).
										addValue(attAge, String.valueOf(visitor.getAge())).
										addValue(attGrade, visitor.getGrade()).
										addValue(attOccupation, visitor.getOccupation()).
										addValue(attNationality, visitor.getNationality()).
										addValue(attGuide, String.valueOf(visitor.isGuide())).
										addValue(attGroup, String.valueOf(visitor.getGroupid()));
									
									if(initReaders) {
										String reader = tokens.get(2).replace("ip=", "");
										createEdgeNodeAndReader(sourceNode, reader);
									}
									
								} else {
									// Se esiste già un nodo con id uguale a idPersonSourceNode allora
									// lo recupero per permettere la creazione dell'arco più avanti.
									sourceNode = listNodes.get(indexSearchedNode.intValue());
									
									if(initReaders) {
										String reader = tokens.get(2).replace("ip=", "");
										createEdgeNodeAndReader(sourceNode, reader);
									}
								}
								
							} else {
								logger.error("No match for the tag " + idBadgeSourceNode + " for TS " + currentTimestamp);
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
							List<Node> listNodes = graph.getNodes();
							List<Edge> listEdges = graph.getAllEdges();
							String idBadgeTargetNode = tokens.get(i).replaceAll("\\x5b|\\x28\\d+\\x29", "");
							
							int indexNodeDB = searchVisitorInDB(Integer.parseInt(idBadgeTargetNode), currentTimestamp);
							if(indexNodeDB >= 0) {
							
								Visitor visitor = usersDB.get(indexNodeDB);
								idPersonTargetNode = visitor.getPersonid();
								
								// Se esiste una corrispondenza tra idBadgeTargetNode ed un tag
								// presente nel DB avente medesimo ID, allora creo il nodo.
								Integer indexSearchedNode = searchIndexNode(listNodes, idPersonTargetNode);
								
								// Se non esiste un nodo con id  uguale a
								// idBadgeTargetNode allora provvedo a crearlo.
								if(indexSearchedNode.equals(-1)) {
									targetNode = graph.createNode(String.valueOf(idPersonTargetNode));
									targetNode.setLabel(String.valueOf(idPersonTargetNode));
									targetNode.getAttributeValues().
										addValue(attBadgeid, String.valueOf(visitor.getBadgeid())).
										addValue(attSex, visitor.getSex()).
										addValue(attAge, String.valueOf(visitor.getAge())).
										addValue(attGrade, visitor.getGrade()).
										addValue(attOccupation, visitor.getOccupation()).
										addValue(attNationality, visitor.getNationality()).
										addValue(attGuide, String.valueOf(visitor.isGuide())).
										addValue(attGroup, String.valueOf(visitor.getGroupid()));
									
									if(initReaders) {
										String reader = tokens.get(2).replace("ip=", "");
										createEdgeNodeAndReader(targetNode, reader);
									}
									
								} else {
									// Se esiste già un nodo con id uguale a idBadgeTargetNode allora
									// lo recupero per permettere la creazione dell'arco più avanti.
									targetNode = listNodes.get(indexSearchedNode.intValue());
									
									if(initReaders) {
										String reader = tokens.get(2).replace("ip=", "");
										createEdgeNodeAndReader(targetNode, reader);
									}
								}
									
							} else {
								logger.error("No match for the tag " + idBadgeTargetNode + " for TS " + currentTimestamp);
							}
							
							
							// Creazione arco tra sourceNode e targetNode
							if(!idPersonSourceNode.equals(-1) && !idPersonTargetNode.equals(-1)) {
								logger.debug("Check s-t: " + sourceNode.getId() + " - " + idPersonTargetNode);
								logger.debug("Check t-s: " + targetNode.getId() + " - " + idPersonSourceNode);
								if(!sourceNode.hasEdgeTo(String.valueOf(idPersonTargetNode)) && !targetNode.hasEdgeTo(String.valueOf(idPersonSourceNode))) {
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
		}
	}
	
	
	private void weightNormalizing() {
		float totalTime = stopRealTS - startRealTS;
		List<Edge> edgeList = graph.getAllEdges();
		for(int i = 0; i < edgeList.size(); i++) {
			edgeList.get(i).setWeight(edgeList.get(i).getWeight() * 1000 / totalTime);
		}
	}
	
	
	private void setWeightExistingEdges(Integer indexExistEdge) {
		List<it.uniroma1.dis.wiserver.gexf4j.core.Edge> edgeList = graph.getAllEdges();	
		edgeList.get(indexExistEdge).setWeight(edgeList.get(indexExistEdge).getWeight() + 1);
	}
	
	
	private Integer searchIndexEdge(List<it.uniroma1.dis.wiserver.gexf4j.core.Edge> listEdges, Node a, Node b) {
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
	
	
	private Integer searchIndexNode(List<Node> listNodes, Integer idPerson) {
		Integer indexExistNode = -1;
		String currentId = String.valueOf(idPerson);
		
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
	
	
	private int searchVisitorInDB(Integer id, Integer currentTimestamp) {
		int found = -1;
		
		for(int i = 0; i < usersDB.size(); i++) {
			Visitor visitor = usersDB.get(i);
			
			if(id.equals(Integer.valueOf(visitor.getBadgeid())) && currentTimestamp >= visitor.getStartdate()) {
				if(visitor.getEnddate() == -1 || currentTimestamp <= visitor.getEnddate()) {
					found = i;
					break;
				}
			}
		}
		
		return found;
	}
	
	
	private ArrayList<Visitor> readDB() {
		ArrayList<Visitor> visitorDB = new ArrayList<Visitor>();
		
		DBObject db = new DBObject(DBConstants.DB_TYPE, DBConstants.DB_SERVER_HOSTNAME, DBConstants.DB_SERVER_PORT, DBConstants.MACRO_DB_NAME, DBConstants.MACRO_DB_USERNAME, DBConstants.MACRO_DB_PASSWORD);
		Connection connection = null;
		try {
			
			connection = db.getConnection();			
			logger.info("Connected to database " + DBConstants.MACRO_DB_NAME + " @ " + DBConstants.DB_SERVER_HOSTNAME);
			
			Statement statement = connection.createStatement();
			String querySelect = "SELECT * FROM " + DBConstants.MACRO_USER_TABLE;
			ResultSet rsSelect = statement.executeQuery(querySelect);
			
			while(rsSelect.next()) {
				Visitor visitor = new Visitor(
						rsSelect.getInt("personid"),
						rsSelect.getInt("badgeid"),
						rsSelect.getString("sex"),
						rsSelect.getInt("age"),
						rsSelect.getString("grade"),
						rsSelect.getString("occupation"),
						rsSelect.getString("nationality"),
						rsSelect.getBoolean("guide"),
						rsSelect.getInt("groupid"),
						rsSelect.getInt("startdate"),
						rsSelect.getInt("enddate"),
						null // ReaderIDs
						);
				
				visitorDB.add(visitor);
			}
			
			statement.close();
			Collections.sort(visitorDB);
			
			for(int i = 0; i < visitorDB.size(); i++) {
				logger.debug(visitorDB.get(i).toString());
			}
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		} finally {
			if (connection != null) {
		    	  try {
					connection.close();
					logger.info("Disconnected to database " + DBConstants.MACRO_DB_NAME + " @ " + DBConstants.DB_SERVER_HOSTNAME);
				} catch (SQLException e) {
					logger.error(e.getMessage());
					//e.printStackTrace();
				}
			}
	    }
		
		return visitorDB;
	}
	

	@Override
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
	

	@Override
	public void writeFile(String outputFileName, String separator) throws IOException {
		logger.info("Writing...");
		
		weightNormalizing();
		
		StaxGraphWriter graphWriter = new StaxGraphWriter();
		File f = new File(fileInput.getParentFile() + "/" + outputFileName);
		FileOutputStream fos = new FileOutputStream(f, false);
		
		graphWriter.writeToStream(xmlGraph, fos);
		
		logger.info("Output file: " + f.getAbsolutePath());
	}

	
	
	/**
	 * INNER CLASS
	 */
	
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

		@Override
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
}
