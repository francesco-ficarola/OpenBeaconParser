package it.uniroma1.dis.wsngroup.extensions.dynamic;


public class Edge {
	private String timestamp;
	private Node source;
	private Node target;
	
	public Edge(String ts, Node s, Node t) {
		this.timestamp = ts;
		this.source = s;
		this.target = t;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public Node getSource() {
		return source;
	}

	public Node getTarget() {
		return target;
	}
}