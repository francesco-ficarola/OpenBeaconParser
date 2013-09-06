package it.uniroma1.dis.wsngroup.parsing.modules.dynamics;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author Francesco Ficarola
 *
 */

public class DnfModule extends AbstractModule {
	
	private Integer startTS;
	private Integer endTS;
	boolean firstTS;
	private Map<String, NodeRecord> nodesMap;
	private Map<ArrayList<String>, EdgeRecord> edgesMap;
	private Map<String, NodeRecord> sortedNodesMap;
	private Map<String, Integer> nodesLastTSMap;
	private Map<ArrayList<String>, Integer> edgesLastTSMap;

	public DnfModule(File fileInput, FileInputStream fis, boolean createVL) {
		super(fileInput, fis, createVL);
		firstTS = false;
		nodesMap = new HashMap<String, NodeRecord>();
		edgesMap = new HashMap<ArrayList<String>, EdgeRecord>();
		nodesLastTSMap = new HashMap<String, Integer>();
		edgesLastTSMap = new HashMap<ArrayList<String>, Integer>();
	}

	private void fromTimestampViewToGraphView() {
		logger.info("Building the Dynamic Graph...");
		
		Iterator<TimestampObject> it = tsObjectList.iterator();
		while(it.hasNext()) {
			TimestampObject timestampObject = it.next();
			Integer currentTS = Integer.parseInt(timestampObject.getTimestamp());
			
			if(!firstTS) {
				firstTS = true;
				startTS = currentTS;
			}
			
			endTS = currentTS;
			
			/** NODES */
			ArrayList<Tag> tags = timestampObject.getTags();
			if(tags != null) {
				for(int i = 0; i < tags.size(); i++) {
					String tagID = tags.get(i).getTagID();
					/** Node already created */
					if(nodesMap.containsKey(tagID)) {
						NodeRecord nodeRecord = nodesMap.get(tagID);
						LinkedList<Object> nodeTimestampGaps = nodeRecord.getGaps();
						
						/** Caso di timestamp non contigui */
						if(!currentTS.equals(nodesLastTSMap.get(tagID)+1)) {
							Integer gapTS = currentTS - nodesLastTSMap.get(tagID);
							nodeTimestampGaps.add(gapTS);
						}
						/** Caso di timestamp contigui */
						else {
							if(nodeTimestampGaps.getLast() instanceof String) {
								/** Caso in cui l'ultimo elemento e' una stringa "+gap". */
								String continuousGap = (String) nodeTimestampGaps.getLast();
								Integer continuosGapValue = Integer.parseInt(continuousGap.substring(1));
								continuosGapValue++;
								nodeTimestampGaps.removeLast();
								String newContinuosGap = "+" + continuosGapValue;
								nodeTimestampGaps.add(newContinuosGap);
							} else if(nodeTimestampGaps.getLast() instanceof Integer) {
								/** 
								 * Caso in cui l'ultimo elemento e' un intero ed e' pari a 1.
								 * Di conseguenza lo elimino e creo una stringa "+2" per
								 * definire un gap di 2 (ovvero il precedente 1 piu' il presente).
								 */
								Integer currentContInteger = (Integer) nodeTimestampGaps.getLast();
								if(currentContInteger.equals(1)) {
									nodeTimestampGaps.removeLast();
									String continuousGap = "+2";
									nodeTimestampGaps.add(continuousGap);
								} else {
									/**
									 * Caso in cui il precedente gap non e' pari ad 1, quindi
									 * e' il primo ad essere il contiguo rispetto al precedente.
									 */
									nodeTimestampGaps.add(1);
								}
							}
						}
						
						nodesLastTSMap.remove(tagID);
					}
					/** New node */
					else {
						LinkedList<Object> nodeTimestampGaps = new LinkedList<Object>();
						Integer initTS = currentTS - startTS;
						nodeTimestampGaps.add(initTS);
						
						NodeRecord nodeRecord = new NodeRecord(tagID, nodeTimestampGaps);
						nodesMap.put(tagID, nodeRecord);
					}
					
					nodesLastTSMap.put(tagID, currentTS);
				}
			}
			
			/** EDGES */
			ArrayList<Edge> edges = timestampObject.getEdges();
			if(edges != null) {
				for(int i = 0; i < edges.size(); i++) {
					ArrayList<String> edge = edges.get(i).getSourceTarget();
					ArrayList<String> edgeInv = new ArrayList<String>();
					if(edge != null) {
						if(edge.size() > 1) {
							edgeInv.add(edge.get(1));
							edgeInv.add(edge.get(0));
						}
					}
					
					/** Edge already created */
					if(edgesMap.containsKey(edge)) {
						EdgeRecord edgeRecord = edgesMap.get(edge);
						LinkedList<Object> edgeTimestampGaps = edgeRecord.getGaps();

						/** Caso di timestamp non contigui */
						if(!currentTS.equals(edgesLastTSMap.get(edge)+1)) {
							Integer gapTS = currentTS - edgesLastTSMap.get(edge);
							edgeTimestampGaps.add(gapTS);
						}
						/** Caso di timestamp contigui */
						else {
							if(edgeTimestampGaps.getLast() instanceof String) {
								/** Caso in cui l'ultimo elemento e' una stringa "+gap". */
								String continuousGap = (String) edgeTimestampGaps.getLast();
								Integer continuosGapValue = Integer.parseInt(continuousGap.substring(1));
								continuosGapValue++;
								edgeTimestampGaps.removeLast();
								String newContinuosGap = "+" + continuosGapValue;
								edgeTimestampGaps.add(newContinuosGap);
							} else if(edgeTimestampGaps.getLast() instanceof Integer) {
								/** 
								 * Caso in cui l'ultimo elemento e' un intero ed e' pari a 1.
								 * Di conseguenza lo elimino e creo una stringa "+2" per
								 * definire un gap di 2 (ovvero il precedente 1 piu' il presente).
								 */
								Integer currentContInteger = (Integer) edgeTimestampGaps.getLast();
								if(currentContInteger.equals(1)) {
									edgeTimestampGaps.removeLast();
									String continuousGap = "+2";
									edgeTimestampGaps.add(continuousGap);
								} else {
									/**
									 * Caso in cui il precedente gap non e' pari ad 1, quindi
									 * e' il primo ad essere il contiguo rispetto al precedente.
									 */
									edgeTimestampGaps.add(1);
								}
							}
						}
						
						edgesLastTSMap.remove(edge);
						edgesLastTSMap.put(edge, currentTS);
						
					}
					
					else 
					
					if(edgesMap.containsKey(edgeInv)){
						EdgeRecord edgeInvRecord = edgesMap.get(edgeInv);
						LinkedList<Object> edgeInvTimestampGaps = edgeInvRecord.getGaps();
						
						/** Caso di timestamp non contigui */
						if(!currentTS.equals(edgesLastTSMap.get(edgeInv)+1)) {
							Integer gapTS = currentTS - edgesLastTSMap.get(edgeInv);
							edgeInvTimestampGaps.add(gapTS);
						}
						/** Caso di timestamp contigui */
						else {
							if(edgeInvTimestampGaps.getLast() instanceof String) {
								/** Caso in cui l'ultimo elemento e' una stringa "+gap". */
								String continuousGap = (String) edgeInvTimestampGaps.getLast();
								Integer continuosGapValue = Integer.parseInt(continuousGap.substring(1));
								continuosGapValue++;
								edgeInvTimestampGaps.removeLast();
								String newContinuosGap = "+" + continuosGapValue;
								edgeInvTimestampGaps.add(newContinuosGap);
							} else if(edgeInvTimestampGaps.getLast() instanceof Integer) {
								/** 
								 * Caso in cui l'ultimo elemento e' un intero ed e' pari a 1.
								 * Di conseguenza lo elimino e creo una stringa "+2" per
								 * definire un gap di 2 (ovvero il precedente 1 piu' il presente).
								 */
								Integer currentContInteger = (Integer) edgeInvTimestampGaps.getLast();
								if(currentContInteger.equals(1)) {
									edgeInvTimestampGaps.removeLast();
									String continuousGap = "+2";
									edgeInvTimestampGaps.add(continuousGap);
								} else {
									/**
									 * Caso in cui il precedente gap non e' pari ad 1, quindi
									 * e' il primo ad essere il contiguo rispetto al precedente.
									 */
									edgeInvTimestampGaps.add(1);
								}
							}
						}
						
						
						edgesLastTSMap.remove(edgeInv);
						edgesLastTSMap.put(edgeInv, currentTS);
					}
					
					/** New Edge */
					else {
						LinkedList<Object> edgeTimestampGaps = new LinkedList<Object>();
						Integer initTS = currentTS - startTS;
						edgeTimestampGaps.add(initTS);
						
						EdgeRecord edgeRecord = new EdgeRecord(edge, edgeTimestampGaps);
						edgesMap.put(edge, edgeRecord);
						edgesLastTSMap.put(edge, currentTS);
					}
				}
			}
		}
		
		sortedNodesMap = new TreeMap<String, NodeRecord>(nodesMap);
	}
	
	@Override
	public void readLog(Integer startTS, Integer endTS) throws IOException {
		super.readLog(startTS, endTS);
		fromTimestampViewToGraphView();
	}

	public void writeFile(String outputFileName, String separator) throws IOException {
		
		File f = new File(fileInput.getParentFile() + "/" + outputFileName);
		FileOutputStream fos = new FileOutputStream(f, true);
		PrintStream ps = new PrintStream(fos);
		
		/** Writing header */
		logger.info("Writing...");
		
		ps.println("graphtype:{dynamic}, defaultedgetype:{undirected}");
		ps.println("dynamics:{start=" +  startTS + "," + "end=" + endTS + "}");
		ps.println("nodeattrs:{}, edgeattrs:{}");
		ps.println();
		
		/** Writing nodes */
		Set<String> listNodes  = sortedNodesMap.keySet();
		Iterator<String> itNodes = listNodes.iterator();
		while(itNodes.hasNext()) {
			String key = itNodes.next();
			NodeRecord nodeRecord = nodesMap.get(key);
			
			/** Writing id */
			ps.print("[" + nodeRecord.getNode() + "]");
			ps.print(" ");
			
			/** Writing gaps */
			LinkedList<Object> nodeGaps = nodeRecord.getGaps();
			Iterator<Object> itNodeGaps = nodeGaps.iterator();
			Integer countNodeGap = 0;
			while(itNodeGaps.hasNext()) {
				Object gap = itNodeGaps.next();
				if(countNodeGap.equals(0) && nodeGaps.size() > 1) {
					ps.print("(" + gap + ",");
				} else
				if(countNodeGap.equals(0) && nodeGaps.size() == 1) {
					ps.println("(" + gap + ")");
				} else
				if(countNodeGap.equals(nodeGaps.size()-1)) {
					ps.println(gap + ")");
				} else {
					ps.print(gap + ",");
					
				}
				countNodeGap++;
			}
		}
		
		ps.println();
		
		/** Writing edges */
		Set<ArrayList<String>> listEdges = edgesMap.keySet();
		Iterator<ArrayList<String>> itEdges = listEdges.iterator();
		while(itEdges.hasNext()) {
			ArrayList<String> key = itEdges.next();
			EdgeRecord edgeRecord = edgesMap.get(key);

			/** Writing id */
			ps.print("[" + edgeRecord.getEdge().get(0) + "," + edgeRecord.getEdge().get(1) + "]");
			ps.print(" ");
			
			/** Writing gaps */
			LinkedList<Object> edgeGaps = edgeRecord.getGaps();
			Iterator<Object> itEdgeGaps = edgeGaps.iterator();
			Integer countEdgeGap = 0;
			while(itEdgeGaps.hasNext()) {
				Object gap = itEdgeGaps.next();
				if(countEdgeGap.equals(0) && edgeGaps.size() > 1) {
					ps.print("(" + gap + ",");
				} else
				if(countEdgeGap.equals(0) && edgeGaps.size() == 1) {
					ps.println("(" + gap + ")");
				} else
				if(countEdgeGap.equals(edgeGaps.size()-1)) {
					ps.println(gap + ")");
				} else {
					ps.print(gap + ",");
					
				}
				countEdgeGap++;
			}
		}
		
		ps.close();
	}
	
	
	
	/** INNER CLASS */
	
	private class NodeRecord implements Comparable<NodeRecord> {
		
		String node;
		LinkedList<Object> gaps;

		public NodeRecord(String nodeid, LinkedList<Object> gaps) {
			this.node = nodeid;
			this.gaps = gaps;
		}

		public String getNode() {
			return node;
		}

		public LinkedList<Object> getGaps() {
			return gaps;
		}

		public int compareTo(NodeRecord n) {
			return node.compareTo(n.node);
		}
	}
	
	private class EdgeRecord implements Comparable<EdgeRecord> {
		
		private ArrayList<String> edge;
		LinkedList<Object> gaps;
		
		public ArrayList<String> getEdge() {
			return edge;
		}

		public EdgeRecord(ArrayList<String> edge, LinkedList<Object> gaps) {
			this.edge = edge;
			this.gaps = gaps;
		}

		public LinkedList<Object> getGaps() {
			return gaps;
		}

		public int compareTo(EdgeRecord e) {
			return edge.get(0).compareTo(e.edge.get(0));
		}
	}
}
