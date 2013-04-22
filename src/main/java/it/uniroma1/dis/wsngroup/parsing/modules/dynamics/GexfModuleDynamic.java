package it.uniroma1.dis.wsngroup.parsing.modules.dynamics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.dynamic.Spell;
import it.uniroma1.dis.wsngroup.gexf4j.core.dynamic.TimeFormat;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.SpellImpl;
import it.uniroma1.dis.wsngroup.constants.ParsingConstants;

public class GexfModuleDynamic extends AbstractModuleWithoutDB {

	/**
	 * @author Francesco Ficarola
	 *
	 */
	
	private Gexf xmlGraph;
	private Graph graph;
	private int idEdges;
	private boolean firstTS;
	private Integer startRealTS;
	private Integer stopRealTS;
	
	public GexfModuleDynamic(File fileInput, FileInputStream fis, boolean createVL) {
		super(fileInput, fis, createVL);
		
		xmlGraph = new GexfImpl();
		xmlGraph.getMetadata().setLastModified(new Date()).setCreator("Francesco Ficarola").setDescription("The SocialDIS Experiment");
		
		graph = xmlGraph.getGraph();
		graph.setTimeType(TimeFormat.XSDDATETIME);
		graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.DYNAMIC);
		
		
		idEdges = 0;
		
		firstTS = false;
		startRealTS = 0;
		stopRealTS = 0;
	}
	
	private void weightNormalizing() {
		float totalTime = stopRealTS - startRealTS;
		List<it.uniroma1.dis.wsngroup.gexf4j.core.Edge> edgeList = graph.getAllEdges();
		for(int i = 0; i < edgeList.size(); i++) {
			edgeList.get(i).setWeight(edgeList.get(i).getWeight() * 1000 / totalTime);
		}
	}
	
	private void setWeightExistingEdges(Integer indexExistEdge) {
		List<it.uniroma1.dis.wsngroup.gexf4j.core.Edge> edgeList = graph.getAllEdges();	
		edgeList.get(indexExistEdge).setWeight(edgeList.get(indexExistEdge).getWeight() + 1);
	}
	
	private Integer searchIndexEdge(List<it.uniroma1.dis.wsngroup.gexf4j.core.Edge> listEdges, Node a, Node b) {
		Integer indexExistEdge = -1;
		if(listEdges != null) {
			for(int i = 0; i < listEdges.size(); i++) {
				if((listEdges.get(i).getSource().getId().equals(a.getId()) && listEdges.get(i).getTarget().getId().equals(b.getId())) ||
					(listEdges.get(i).getSource().getId().equals(b.getId()) && listEdges.get(i).getTarget().getId().equals(a.getId()))) {
					indexExistEdge = i;
					break;
				}
			}
		}
		return indexExistEdge;
	}
	
	private Integer searchIndexNode(List<Node> listNodes, String currentId) {
		Integer indexExistNode = -1;
		
		if(listNodes != null) {
			for(int i = 0; i < listNodes.size(); i++) {
				if(listNodes.get(i).getId().equals(currentId)) {
					indexExistNode = i;
					break;
				}
			}
		}
		return indexExistNode;
		
	}

	public void writeFile(String outputFileName, String separator) throws IOException {
		if(tsObjectList.size() > 0) {
			logger.info("Building the GEXF Graph...");
			
			// Timestamps data
			HashMap<String, Node> hashMapNodes = new HashMap<String, Node>();
			Iterator<TimestampObject> it = tsObjectList.iterator();
			while(it.hasNext()) {
				List<Node> listNodes = graph.getNodes();
				List<it.uniroma1.dis.wsngroup.gexf4j.core.Edge> listEdges = graph.getAllEdges();
				TimestampObject timestampObject = it.next();
				Long milliCurrentTS = Long.parseLong(timestampObject.getTimestamp()) * ParsingConstants.MILLI;
				Date currentDate = new Date(milliCurrentTS.longValue());
				
				// Aggiunta dei tag al grafo o, se esistenti,
				// aggiornamento dei timestamp in cui sono presenti.
				ArrayList<Tag> currentTagsList = timestampObject.getTags();
				for(int i = 0; i < currentTagsList.size(); i++) {
					String idCurrentNode = String.valueOf(currentTagsList.get(i).getTagID());
					Integer indexSearchedNode = searchIndexNode(listNodes, idCurrentNode);
					
					if(indexSearchedNode.equals(-1)) {
						Node newNode = graph.createNode(idCurrentNode);
						newNode.setLabel(idCurrentNode);
						Spell spell = new SpellImpl();
						spell.setStartValue(currentDate);
						spell.setEndValue(currentDate);
						newNode.getSpells().add(spell);
						hashMapNodes.put(newNode.getId(), newNode);
					} else {
						Node existingNode = listNodes.get(indexSearchedNode.intValue());
						if(existingNode.getSpells() != null) {
							List<Spell> listSpells = existingNode.getSpells();
							if(listSpells.size() > 0) {
								Spell lastSpell = listSpells.get(listSpells.size()-1);
								long prevTime = ((Date) lastSpell.getEndValue()).getTime() + (1 * ParsingConstants.MILLI);
								if(currentDate.getTime() == prevTime) {
									lastSpell.setEndValue(currentDate);
								} else {
									Spell spell = new SpellImpl();
									spell.setStartValue(currentDate);
									spell.setEndValue(currentDate);
									listSpells.add(spell);
								}
							}
						}
					}
				}
				
				ArrayList<Edge> currentEdgesList = timestampObject.getEdges();
				for(int i = 0; i < currentEdgesList.size(); i++) {
					Edge currentEdge = currentEdgesList.get(i);
					String idSourceNode = String.valueOf(currentEdge.getSourceTarget().get(0));
					String idTargetNode = String.valueOf(currentEdge.getSourceTarget().get(1));
					if(hashMapNodes.containsKey(idSourceNode) && hashMapNodes.containsKey(idTargetNode)) {
						Node source = hashMapNodes.get(idSourceNode);
						Node target = hashMapNodes.get(idTargetNode);
						if(!source.hasEdgeTo(idTargetNode) && !target.hasEdgeTo(idSourceNode)) {
							it.uniroma1.dis.wsngroup.gexf4j.core.Edge newEdge = source.connectTo(String.valueOf(idEdges), target).setWeight(1);
							Spell spell = new SpellImpl();
							spell.setStartValue(currentDate);
							spell.setEndValue(currentDate);
							newEdge.getSpells().add(spell);
							idEdges++;
						} else {
							Integer indexSearchEdge = searchIndexEdge(listEdges, source, target);
							if(!indexSearchEdge.equals(-1)) {
								it.uniroma1.dis.wsngroup.gexf4j.core.Edge existingEdge = listEdges.get(indexSearchEdge.intValue());
								setWeightExistingEdges(indexSearchEdge);
								if(existingEdge.getSpells() != null) {
									List<Spell> listSpells = existingEdge.getSpells();
									if(listSpells.size() > 0) {
										Spell lastSpell = listSpells.get(listSpells.size()-1);
										long prevTime = ((Date) lastSpell.getEndValue()).getTime() + (1 * ParsingConstants.MILLI);
										if(currentDate.getTime() == prevTime) {
											lastSpell.setEndValue(currentDate);
										} else {
											Spell spell = new SpellImpl();
											spell.setStartValue(currentDate);
											spell.setEndValue(currentDate);
											listSpells.add(spell);
										}
									}
								}
							} else {
								logger.error("Edge in the HashMap, but not in the graph.");
							}
						}
					}
						
				}
				
				// Memorizzazione del timestamp iniziale e finale
				if(!firstTS) {
					firstTS = true;
					startRealTS = Integer.parseInt(timestampObject.getTimestamp());
				}
				stopRealTS = Integer.parseInt(timestampObject.getTimestamp());
			}
			
			// Normalizzazione dei pesi sugli archi
			weightNormalizing();
			
			
			logger.info("Writing...");
			
			StaxGraphWriter graphWriter = new StaxGraphWriter();
			File f = new File(fileInput.getParentFile() + "/" + outputFileName);
			FileOutputStream fos = new FileOutputStream(f, true);
			graphWriter.writeToStream(xmlGraph, fos, "UTF-8");
			logger.info("Output file: " + f.getAbsolutePath());
		}
	}
}
