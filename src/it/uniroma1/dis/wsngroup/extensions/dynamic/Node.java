package it.uniroma1.dis.wsngroup.extensions.dynamic;

import java.util.LinkedList;

public class Node {
	private int ID;
	private int tagID;
//	private LinkedList<Edge> edges;
//	private LinkedList<NodeLink> edges;
	private LinkedList<Node> tempEdges;
	private int count;
	private boolean hasToken;
	
	public Node(int id, int tagid) {
		this.ID = id;
		this.tagID = tagid;
		this.tempEdges = new LinkedList<Node>();
//		this.edges = new LinkedList<NodeLink>();
		this.count = 0;
		this.hasToken = false;
	}
	
	public Node (int tagid) {
		this.ID = 0;
		this.tagID = tagid;
		this.tempEdges = new LinkedList<Node>();
//		this.edges = new LinkedList<NodeLink>();
		this.count = 0;
		this.hasToken = false;
	}

	public int getID() {
		return ID;
	}

	public int getTagID() {
		return tagID;
	}
	
	public LinkedList<Node> getTempEdges() {
		return tempEdges;
	}
	
	public void addTempEdge(Node n) {
		this.tempEdges.add(n);
	}
	
	public void resetTempEdges() {
		this.tempEdges.clear();
	}

	public int getCount() {
		return count;
	}

	public void addToken() {
		this.count++;
		this.hasToken = true;
	}

//	public LinkedList<NodeLink> getEdges() {
//		return this.edges;
//	}

//	public void addEdge(NodeLink edge) {
//		this.edges.add(edge);
//	}

	public boolean hasToken() {
		return hasToken;
	}
	
	public void dropToken() {
		this.hasToken = false;
	}
	
	protected class NodeLink {
		private String timestamp;
		private Node target;
		
		public NodeLink(String ts, Node t) {
			timestamp = ts;
			target = t;
		}
		
		public String getTimestamp() {
			return timestamp;
		}

		public Node getTarget() {
			return target;
		}
	}
}