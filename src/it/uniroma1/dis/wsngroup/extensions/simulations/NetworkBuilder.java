package it.uniroma1.dis.wsngroup.extensions.simulations;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import cern.colt.matrix.DoubleMatrix2D;

import com.google.gson.Gson;

/** These are the networks for which it is possible to run simulations, 
 * provided that you have the corresponding JSON data files:
 * - DIS: network associated to the SocialDIS project
 * - MACRO: network associated to the MACRO project
 * - TWITTER: artificial @reply network created from Twitter data 
 * - MEMETRACKER: network obtained by raw phrases data at www.memetracker.org
 */
enum Network {
	DIS, MACRO, TWITTER, MEMETRACKER;
}

/** This class provides functionalities for simulating decentralized algorithms
 * for the computation of importance measures of nodes in specific evolving networks
 * represented by means of JSON files
 */
public class NetworkBuilder {
	private File inputJSONFile;
	private FileReader reader;
	private Network selectedNetwork;
	private Gson gson;
	private JsonEntry[] entries;
	private int numEntries;
	private List<Node> nodes;
	private HashMap<Long, Node> map; // mapping between a Node object and its id
	private double generateProbability; //probability that a node generates a token
	private double forwardProbability; //probability that a node forwards a token to one of its neighbors
	private int totalCWPsum;
	private int totalCWEsum;
	private boolean estimatePageRanks;
	private boolean estimateDegreeCentrality;
	
	private NetworkBuilder(Network n, boolean pr, boolean dc) throws IOException {
		selectedNetwork = n;
		gson = new Gson();
		numEntries = 0;
		nodes = new ArrayList<Node>();
		map = new HashMap<Long, Node>();
		generateProbability = 0.15;
		forwardProbability = 0.85;
		totalCWPsum = 0;
		totalCWEsum = 0;
		estimatePageRanks = pr;
		estimateDegreeCentrality = dc;
	}
	
	public NetworkBuilder(File f, Network n, boolean pr, boolean dc) throws IOException {
		this(n, pr, dc);
		inputJSONFile = f;
		reader = new FileReader(inputJSONFile);
		entries = null;
		
		initNodes();
	}
	
	public NetworkBuilder(JsonEntry[] e, File f, Network n, boolean pr, boolean dc) throws IOException {
		this(n, pr, dc);
		entries = e;
		inputJSONFile = f;
		reader = new FileReader(inputJSONFile);
		
		initNodes();
	}
	
	public NetworkBuilder(File f, Network n, DoubleMatrix2D w, boolean pr, boolean dc) throws IOException {
		this(n, pr, dc);
		inputJSONFile = f;
		reader = new FileReader(inputJSONFile);
		entries = null;
		
		initNodes();
	}
	
	/** Scans the input JSON file and initializes the nodes of the network */ 
	private void initNodes() throws IOException {
		if (reader == null || selectedNetwork == null) {
			System.out.println("Initialization problem, aborting");
			System.exit(-1);
		}
		
		if (entries == null) {
			BufferedReader br = new BufferedReader(reader);
			
			System.out.println("\nReading data from JSON file and initializing...");
			switch(selectedNetwork) {
			case DIS:
				entries = gson.fromJson(br, DisEntry[].class);
				break;
			case MACRO:
				entries = gson.fromJson(br, MacroEntry[].class);
				break;
			case TWITTER:
				entries = gson.fromJson(br, TwitterEntry[].class);
				break;
			case MEMETRACKER:
				entries = gson.fromJson(br, MemeTrackerEntry[].class);
				break;
			default:
				System.out.println("Error, undefined network");
				System.exit(-1);
			}
		}
		
		numEntries = entries.length;
		
		int N; // number of nodes in the network
		int bitmapSize, numHashes;
		
		// Parameters of the FM sketch implementation
		N = 150;
		bitmapSize = (int)Math.ceil(Math.log(N));
		numHashes = 4;
		
		FM_HashFunction[] hashes = null;
		
		if (estimateDegreeCentrality)
			hashes = FlajoletMartinSketch.generateFMHashFunctions(bitmapSize, numHashes);
		
		Node n = null;
		
		for (int i = 0; i < numEntries; i++) {
			switch(selectedNetwork) {
			case DIS:
				for (Tag t : ((DisEntry)entries[i]).getTags()) {
					if (!map.containsKey(t.getTagID())) {
						n = new Node(t.getTagID());
						map.put(t.getTagID(), n);
						nodes.add(n);
						
						if (estimateDegreeCentrality)
							n.initializeSketch(bitmapSize, hashes);
					}
				}
				break;
				
			case MACRO:
				for (Visitor v : ((MacroEntry)entries[i]).getVisitors()) {
					if (!map.containsKey(v.getPersonID())) {
						n = new Node(v.getPersonID());
						map.put(v.getPersonID(), n);
						nodes.add(n);
						
						if (estimateDegreeCentrality)
							n.initializeSketch(bitmapSize, hashes);
					}
				}
				break;
				
			case TWITTER:
				for (TwitterUser u : ((TwitterEntry)entries[i]).getUsers()) {
					if (!map.containsKey(u.getUserID())) {
						n = new Node(u.getUserID(), u.getScreenName());					
						map.put(u.getUserID(), n);
						nodes.add(n);
						
						if (estimateDegreeCentrality)
							n.initializeSketch(bitmapSize, hashes);
					}
				}
				break;
				
			case MEMETRACKER:
				for (Document d : ((MemeTrackerEntry)entries[i]).getDocuments()) {
					if (!map.containsKey((long)d.getHostURL().hashCode())) {						
						n = new Node(d.getHostURLid(), d.getHostURL());
						map.put(d.getHostURLid(), n);
						nodes.add(n);
						
						if (estimateDegreeCentrality)
							n.initializeSketch(bitmapSize, hashes);
					}
				}
				break;
				
			default:
				System.out.println("Error, undefined network");
				System.exit(-1);
			}
		}
	}
	
	/**
	 * Simulates the selected algorithms on the real network traces,
	 * thus considering in each time step the actual connections between
	 * nodes which exist in the network in that specific time step
	 */
	public void simulateRealTracesNetwork() throws FileNotFoundException {
		LinkedList<Node> nodesInTimeStep = new LinkedList<Node>();
		
		System.out.println("Simulating the algorithms on the real network trace...");
		
		for (int i = 0; i < entries.length; i++) {
//			if (i % 1000 == 0) System.out.println("Processing entry " + i);
			
			for (EdgeEntry ee : entries[i].getEdges()) {
				long[] edgeEntry = ee.getSourceTarget();
				if (!map.containsKey(edgeEntry[0]) || !map.containsKey(edgeEntry[1])) {
					System.out.println("ERROR: a node has an edge but it's not registered");
					System.exit(-1);
				}
				
				Node s = map.get(edgeEntry[0]);
				Node t = map.get(edgeEntry[1]);
				
				switch(selectedNetwork) {
					case DIS: // DIS and MACRO are undirected graphs...
					case MACRO:
						t.addTempEdge(s);
						s.incrementInDegree();
						t.incrementOutDegree();
						
						s.addEncountered(t.getID());
						// no break
						
					case TWITTER: // ...while TWITTER and MEMETRACKER are directed
					case MEMETRACKER:
						s.addTempEdge(t);
						s.incrementOutDegree();
						t.incrementInDegree();
						
						t.addEncountered(s.getID());
						break;
						
					default:
						System.out.println("Error, undefined network");
						System.exit(-1);
				}
			}
			
			if (estimatePageRanks)
				CWAlgorithms(nodesInTimeStep); 
			
			if (estimateDegreeCentrality) {
				DUCAlgorithm(nodesInTimeStep);
				ENSAlgorithm(nodesInTimeStep);
			}
			
			removeTempEdges(nodesInTimeStep);
			nodesInTimeStep.clear();
		}
		
		for (Node n : nodes) {
			n.setCWPestimation((double)n.getCWPcount()/totalCWPsum);
			n.setCWEestimation((double)n.getCWEcount()/totalCWEsum);
		}
	}
	
	/** Simulates at each node the generation of a token */
	public void generate() {
		for (Node n : nodes) {
			Random generator = new Random();
			double p = generator.nextDouble();
			if (p <= generateProbability) 
				n.addToken();
		}
	}
	
	/** Simulates the CWP and CWE algorithms for computing a PageRank-like measure */
	public void CWAlgorithms(List<Node> timestepNetwork) {
		generate();
		
		// forward
		for (Node n : nodes) {
			if (!n.getTempEdges().isEmpty()) { // if there are no contacts, the node does nothing
				while (n.getTokens() > 0) {
					Random generator = new Random();
					double p = generator.nextDouble();
					if (p <= forwardProbability) {
						Random generator2 = new Random();
						double p2 = generator2.nextInt(n.getTempEdges().size());
						
						n.getTempEdges().get((int)p2).addToken();
						
						n.getTempEdges().get((int)p2).incrementCWPCount();
						totalCWPsum++;
					}
					else {
						n.incrementCWECount();
						totalCWEsum++;
					}
					
					n.removeToken();
				}
			}
		}
			
	}
	
	/** Simulates the DUC algorithm for estimating at each node the number of distinct interactions */
	public void DUCAlgorithm(List<Node> timestepNetwork) {
		for (Node n : timestepNetwork) {
			for (Node no : n.getTempEdges()) {
				no.addIdToDUCsketch(n.getID());
			}
		}
	}
	
	/** 
	 * Simulates the ENS algorithm for estimating at each node 
	 * the size of the connected subcomponent it belongs to 
	 */
	public void ENSAlgorithm(List<Node> timestepNetwork) {
		for (Node n : timestepNetwork) {
			for (Node no : n.getTempEdges()) {
				no.sumSketchToENSsketch(n.getENSsketch());
				no.addToSubnetwork(n.getSubnetwork());
			}
		}
	}
	
	public void removeTempEdges(List<Node> network) {
		for (Node n : network)
			n.resetTempEdges();
	}
	
	public int getJSONentries() {
		return numEntries;
	}

	public String getCWPresults() {
		StringBuffer sb = new StringBuffer("#ID;\"CWP results\";In-degree;Out-degree\n");
		
		Collections.sort(nodes, new NodesCWPComparator());
		for (Node n : nodes) {
			sb.append(n.getID());
			
			if (!n.getName().equals(""))
				sb.append("(" + n.getName() + ")");
			
			sb.append(";" + n.getCWPestimation() + ";" + n.getInDegree() + ";" + n.getOutDegree() + "\n");
		}
		
		return sb.toString();
	}
	
	public String getCWEresults() {
		StringBuffer sb = new StringBuffer("#ID;\"CWE results\";In-degree;Out-degree\n");
		
		Collections.sort(nodes, new NodesCWPComparator());
		for (Node n : nodes) {
			sb.append(n.getID());
			
			if (!n.getName().equals(""))
				sb.append("(" + n.getName() + ")");
			
			sb.append(";" + n.getCWEestimation() + ";" + n.getInDegree() + ";" + n.getOutDegree() + "\n");
		}
		
		return sb.toString();
	}
	
	public String getDUCresults() {
		StringBuffer buf = new StringBuffer("#ID;\"DUC results\";\"distinct users\"\n");
		
		for (Node n : nodes) {
			buf.append(n.getID() + ";");
			buf.append(n.getDUCsketch().estimateCount() + ";");
			buf.append(n.getDistinctUsers() + "\n");
		}
		
		return buf.toString();
	}
	
	public String getENSresults() {
		StringBuffer buf = new StringBuffer("#ID;\"ENS results\";\"subnetwork size\"\n");
		
		for (Node n : nodes) {
			buf.append(n.getID() + ";");
			buf.append(n.getENSsketch().estimateCount() + ";");
			buf.append(n.getSubnetworkSize() + "\n");
		}
		
		return buf.toString();
	}
	
	public JsonEntry[] getJsonEntries() {
		return entries;
	}
	
	public List<Node> getNodes() {
		return nodes;
	}

	protected class JsonEntry {
		private long timestamp;
		private LinkedList<EdgeEntry> edges;
		
		public LinkedList<EdgeEntry> getEdges() {
			return edges;
		}
		
		public long getTimestamp() {
			return timestamp;
		}
	}
	
	private class EdgeEntry {
		private long[] source_target;
		
		public EdgeEntry(long source, long target) {
			source_target = new long[]{source, target};
		}
		
		public long[] getSourceTarget() {
			return source_target;
		}
	}
	
	private class MacroEntry extends JsonEntry {
		private LinkedList<Visitor> visitors;

		public LinkedList<Visitor> getVisitors() {
			return visitors;
		}
	}
	
	private class DisEntry extends JsonEntry {
		private LinkedList<Tag> tags;

		public LinkedList<Tag> getTags() {
			return this.tags;
		}
	}
	
	private class TwitterEntry extends JsonEntry {
		private LinkedList<TwitterUser> users;
		
		public LinkedList<TwitterUser> getUsers() {
			return users;
		}
	}
	
	private class MemeTrackerEntry extends JsonEntry {
		private LinkedList<Document> documents;
		
		public LinkedList<Document> getDocuments() {
			return documents;
		}
	}
	
	private class Visitor {
		private long personid;
		private long badgeid;

		public long getPersonID() {
			return personid;
		}

		public long getBadgeID() {
			return badgeid;
		}
	}
	
	private class Tag {
		private long tagID;

		public long getTagID() {
			return tagID;
		}
	}
	
	private class TwitterUser {
		private long userid;
		private String screen_name;
		private String name;
		
		public long getUserID() {
			return userid;
		}
		
		public String getScreenName() {
			return screen_name;
		}
		
		public String getName() {
			return name;
		}
	}
	
	private class Document {
		private long host_url_id;
		private String host_url;
		
		public long getHostURLid() {
			return host_url_id;
		}
		
		public String getHostURL() {
			return host_url;
		}
	}
}
