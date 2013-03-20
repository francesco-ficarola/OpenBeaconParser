package it.uniroma1.dis.wsngroup.extensions.dynamic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import com.google.gson.Gson;

public class NetworkBuilder {
	private FileReader reader;
	private String network; // 1 = dis, 2 = macro
	private Gson gson;
	private LinkedList<Node> nodes;
	private LinkedList<Edge> edges;
	private HashMap<Integer, Node> map; // mapping between a Node object and its id
	private double generateProbability; //probability that a node generates a token
	private double forwardProbability; //probability that a node forwards a token to one of its neighbors
	
	public NetworkBuilder(FileReader r, String choise) {
		reader = r;
		network = choise;
		gson = new Gson();
		nodes = new LinkedList<Node>();
		edges = new LinkedList<Edge>();
		map = new HashMap<Integer, Node>();
		generateProbability = 0.15;
		forwardProbability = 0.85;
	}
	
	public void simulateNetwork() {
		BufferedReader br = new BufferedReader(reader);
		
		JsonEntry[] entries = null;
		
		if (network.equals("1"))
			entries = gson.fromJson(br, DisEntry[].class);
		else if (network.equals("2"))
			entries = gson.fromJson(br, MacroEntry[].class);
		
		LinkedList<Node> tempNetwork = new LinkedList<Node>();

		for (int i = 0; i < entries.length; i++) {
			if (network.equals("1")) {
				Iterator<Tag> itG = ((DisEntry)entries[i]).getTags().iterator();
				while (itG.hasNext()) {
					Tag t = itG.next();
					if (!map.containsKey(t.getTagID())) {
						Node n = new Node(t.getTagID());
						map.put(t.getTagID(), n);
						nodes.add(n);
					}
					
					tempNetwork.add(map.get(t.getTagID()));
				}
			}
			else if (network.equals("2")) {
				Iterator<Visitor> itV = ((MacroEntry)entries[i]).getVisitors().iterator();
				while (itV.hasNext()) {
					Visitor v = itV.next();
					if (!map.containsKey(v.getPersonid())) {
						Node n = new Node(v.getPersonid(), v.getBadgeid());
						map.put(v.getPersonid(), n);
						nodes.add(n);
					}
				
					tempNetwork.add(map.get(v.getPersonid()));
				}
			}
			
			Iterator<EdgeEntry> it = entries[i].getEdges().iterator();
			while (it.hasNext()) {
				int[] edgeEntry = it.next().getSourceTarget();
				if (!map.containsKey(edgeEntry[0])) {
					System.out.println("ERROR: a node has an edge but it's not registered");
					System.exit(-1);
				}
				
				Node s = map.get(edgeEntry[0]);
				Node t = map.get(edgeEntry[1]);
				
				s.addTempEdge(t);
//				s.addEdge(new NodeLink(entries[i].getTimestamp(), t));
				t.addTempEdge(s);
//				t.addEdge(new NodeLink(entries[i].getTimestamp(), s));
				
				edges.add(new Edge(entries[i].getTimestamp(), s, t));
			}
			
			algorithmStep(tempNetwork);
			removeTempEdges(tempNetwork);
			tempNetwork.clear();
		}
	}
	
	/*public void simulateDisNetwork() {
		BufferedReader br = new BufferedReader(reader);
 
		DisEntry[] entries = gson.fromJson(br, DisEntry[].class);
		LinkedList<Node> tempNetwork = new LinkedList<Node>();
		
		for (int i = 0; i < entries.length; i++) {
			Iterator<Tag> itG = entries[i].getTags().iterator();
			while (itG.hasNext()) {
				Tag t = itG.next();
				if (!map.containsKey(t.getTagID())) {
					Node n = new Node(t.getTagID());
					map.put(t.getTagID(), n);
					nodes.add(n);
				}
				
				tempNetwork.add(map.get(t.getTagID()));
			}
			
			Iterator<EdgeEntry> it = entries[i].getEdges().iterator();
			while (it.hasNext()) {
				int[] edgeEntry = it.next().getSourceTarget();
				if (!map.containsKey(edgeEntry[0])) {
					System.out.println("ERROR: a node has an edge but it's not registered");
					System.exit(-1);
				}
				
				Node s = map.get(edgeEntry[0]);
				Node t = map.get(edgeEntry[1]);
				
				s.addTempEdge(t);
//				s.addEdge(new NodeLink(entries[i].getTimestamp(), t));
				t.addTempEdge(s);
//				t.addEdge(new NodeLink(entries[i].getTimestamp(), s));
				
				edges.add(new Edge(entries[i].getTimestamp(), s, t));
			}
			
			algorithmStep(tempNetwork);
			removeTempEdges(tempNetwork);
			tempNetwork.clear();
		}
	}*/
	
	public void algorithmStep(LinkedList<Node> network) {
		Iterator<Node> it = network.iterator();
		while (it.hasNext()) { // every node tries to generate a token and send it to one of its neighbors
			Node n = it.next();
			if (!n.getTempEdges().isEmpty()) {
				double p = Math.random();
				if (p <= generateProbability) {
					double p2 = Math.random()*n.getTempEdges().size();
					n.getTempEdges().get((int)p2).addToken(); 
				}
			}
		}
		
		boolean someoneHasToken = true;
		
		// !!! Un nodo con più di un token prova a inoltrarli tutti o solo uno? !!!
		while (someoneHasToken) { // the nodes exchange tokens
			someoneHasToken = false;
			Iterator<Node> it2 = network.iterator();
			while (it2.hasNext()) {
				Node n = it2.next();
				if (n.hasToken() && !n.getTempEdges().isEmpty()) {
					double p = Math.random();
					if (p <= forwardProbability) {
						double p2 = Math.random()*n.getTempEdges().size();
						n.getTempEdges().get((int)p2).addToken();
						someoneHasToken = true;
					}
					n.dropToken();
				}
			}
		}
	}
	
	public void removeTempEdges(LinkedList<Node> network) {
		Iterator<Node> it = network.iterator();
		while (it.hasNext())
			it.next().resetTempEdges();
	}
	
	public void printNetwork() {
		Iterator<Node> itN = nodes.iterator();
		System.out.println("NODES:");
		while (itN.hasNext())
			System.out.println(itN.next().getID());
		
		Iterator<Edge> itE = edges.iterator();
		System.out.println("EDGES:");
		while (itE.hasNext()) {
			Edge e = itE.next();
			System.out.println(e.getTimestamp() + ": " + e.getSource().getID() + " -> " +
					+ e.getTarget().getID());
		}
	}
	
	public void printHashMap() {
		for (int key: map.keySet()) {
			System.out.println(key + " -> " + map.get(key).getTagID());
			}
	}
	
	public void printNodesCount() {
		Collections.sort(nodes, new NodesCountComparator());
		Iterator<Node> it = nodes.iterator();
		System.out.println("NODES COUNT:");
		while (it.hasNext()) {
			Node n = it.next();
			System.out.println(n.getTagID() + ": " + n.getCount());
		}
	}
	
	protected class JsonEntry {
		private String timestamp;
		private LinkedList<EdgeEntry> edges;
		
		public String getTimestamp() {
			return this.timestamp;
		}
		
		public LinkedList<EdgeEntry> getEdges() {
			return this.edges;
		}
	}
	
	protected class EdgeEntry {
		private int[] source_target;

		public int[] getSourceTarget() {
			return source_target;
		}
	}
	
	protected class MacroEntry extends JsonEntry {
		private LinkedList<Visitor> visitors;

		public LinkedList<Visitor> getVisitors() {
			return visitors;
		}
	}
	
	protected class DisEntry extends JsonEntry {
		private LinkedList<Tag> tags;

		public LinkedList<Tag> getTags() {
			return this.tags;
		}
	}
	
	protected class Visitor {
		private int personid;
		private int badgeid;

		public int getPersonid() {
			return personid;
		}

		public int getBadgeid() {
			return badgeid;
		}
	}
	
	protected class Tag {
		private int tagID;

		public int getTagID() {
			return tagID;
		}
	}
}

class NodesCountComparator implements Comparator<Node> {
	@Override
	public int compare(Node n1, Node n2) {
		if (n1.getCount() == n2.getCount()) return 0;
		if (n1.getCount() > n2.getCount()) return -1;
		return 1;
	}
}