package it.uniroma1.dis.wsngroup.parsing.modules.aggregate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import it.uniroma1.dis.wsngroup.gexf4j.core.Edge;
import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

/**
 * @author Francesco Ficarola
 *
 */

public class GexfModule implements GraphRepresentation {
	
	private Logger logger = Logger.getLogger(this.getClass());
	
	private File fileInput;
	private FileInputStream fis;
	private Gexf xmlGraph;
	private Graph graph;
	private int indexEdges;
	private Node sourceNode;
	private Node targetNode;
	private boolean firstLogRow;
	private Integer startRealTS;
	private Integer stopRealTS;
	
	public GexfModule(File fileInput, FileInputStream fis) {
		this.fileInput = fileInput;
		this.fis = fis;
		
		// Creazione grafo
		xmlGraph = new GexfImpl();
		xmlGraph.getMetadata().setLastModified(new Date()).setCreator("OpenBeacon Parser").setDescription("The WSDM Experiment");
		
		graph = xmlGraph.getGraph();
		graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);
		
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
		if(tokens.size() > 1 && (Integer.parseInt(tokens.get(1).replace("t=", "")) >= startTS &&
				Integer.parseInt(tokens.get(1).replace("t=", "")) <= endTS)) {
			
			// Memorizzazione del primo e dell'ultimo TS reali
			// (serviranno per calcolare la normalizzazione)
			if(!firstLogRow) {
				startRealTS = Integer.parseInt(tokens.get(1).replace("t=", ""));
				firstLogRow = true;
			} else {
				stopRealTS = Integer.parseInt(tokens.get(1).replace("t=", ""));
			}
			
			
			String idSourceNode = "";
			String idTargetNode = "";
			
			for(int i = 0; i < tokens.size(); i++) {
				
				/* 
				 * RILEVAMENTI
				 */				
				if(tokens.get(0).equals("S")) {
					if(tokens.get(i).contains("id=")) {
						List<Node> listNode = xmlGraph.getGraph().getNodes();
						idSourceNode = tokens.get(i).replace("id=", "");
						Integer indexSearchedNode = searchIndexNode(listNode, idSourceNode);
						
						// Se non esiste un nodo con id  uguale a
						// idSourceNode allora provvedo a crearlo.
						if(indexSearchedNode.equals(-1)) {
							Node node = xmlGraph.getGraph().createNode(idSourceNode);
							node.setLabel(idSourceNode);
						}
					}
				}
				
				else
				
				if(tokens.get(0).equals("C")) {
					
					if(tokens.get(i).contains("id=")) {						
						List<Node> listNode = xmlGraph.getGraph().getNodes();
						idSourceNode = tokens.get(i).replace("id=", "");
						Integer indexSearchedNode = searchIndexNode(listNode, idSourceNode);
						
						// Se non esiste un nodo con id  uguale a
						// idSourceNode allora provvedo a crearlo.
						if(indexSearchedNode.equals(-1)) {
							sourceNode = xmlGraph.getGraph().createNode(idSourceNode);
							sourceNode.setLabel(idSourceNode);
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
							targetNode = xmlGraph.getGraph().createNode(idTargetNode);
							targetNode.setLabel(idTargetNode);
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

}
