package it.uniroma1.dis.wsngroup.parsing.modules.statistics;

import it.uniroma1.dis.wsngroup.parsing.modules.dynamics.AbstractModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class StatsModule extends AbstractModule {

	public StatsModule(File fileInput, FileInputStream fis, boolean createVL) {
		super(fileInput, fis, createVL);
	}
	
	private void printStats() {
		if(tsObjectList.size() > 0) {
			logger.info("*** STATISTICS ***");
			logger.info("Timestamps: " + tsObjectList.size());
			
			Integer numberOfTotalContacts = 0;
			Map<EdgeObject, SortedSet<Integer>> edgesMap = new HashMap<EdgeObject, SortedSet<Integer>>();
			Map<Integer, Integer> contactSeconds = new HashMap<Integer, Integer>();
			
			// Saving edges and their occurring timestamps in a hashmap
			Iterator<TimestampObject> it = tsObjectList.iterator();
			while(it.hasNext()) {
				TimestampObject curTimestampObject = it.next();
				Integer curTimestampNum = Integer.parseInt(curTimestampObject.getTimestamp());
				
				ArrayList<Edge> edges = curTimestampObject.getEdges();
				Iterator<Edge> edgeIt = edges.iterator();
				while(edgeIt.hasNext()) {
					Edge edge = edgeIt.next();
					EdgeObject edgeObject = new EdgeObject(edge.getSourceTarget().get(0), edge.getSourceTarget().get(1));
					EdgeObject edgeObjectInv = new EdgeObject(edge.getSourceTarget().get(1), edge.getSourceTarget().get(0));
					if(edgesMap.containsKey(edgeObject)) {
						SortedSet<Integer> ts = edgesMap.get(edgeObject);
						ts.add(curTimestampNum);
						edgesMap.put(edgeObject, ts);
					}
					else
					if(edgesMap.containsKey(edgeObjectInv)) {
						SortedSet<Integer> ts = edgesMap.get(edgeObjectInv);
						ts.add(curTimestampNum);
						edgesMap.put(edgeObjectInv, ts);
					}
					else {
						SortedSet<Integer> ts = new TreeSet<Integer>();
						ts.add(curTimestampNum);
						edgesMap.put(edgeObject, ts);
					}
					numberOfTotalContacts++;
				}
			}
			
			logger.info("Contacts: " + numberOfTotalContacts);
			
			// Counting contact seconds...
			Set<Map.Entry<EdgeObject, SortedSet<Integer>>> edgesSet = edgesMap.entrySet();
			for(Map.Entry<EdgeObject, SortedSet<Integer>> entry : edgesSet) {
				EdgeObject edgeObject = entry.getKey();
				Set<Integer> ts = entry.getValue();
				Object[] tsArray = ts.toArray();
				logger.debug(edgeObject.toString() + ": " + Arrays.toString(tsArray));
				
				Integer countSeconds = 0;
				if(tsArray.length > 1) {
					for(int i = 0; i < tsArray.length; i++) {
						countSeconds++;
						if(i < tsArray.length - 1) {
							Integer currentTS = (Integer)tsArray[i];
							Integer nextTS = (Integer)tsArray[i+1];
							currentTS++;
							if(!currentTS.equals(nextTS)) {
								if(contactSeconds.containsKey(countSeconds)) {
									Integer curSec = contactSeconds.get(countSeconds);
									curSec++;
									contactSeconds.put(countSeconds, curSec);
								} else {
									contactSeconds.put(countSeconds, 1);
								}
								logger.debug("[INEQ] " + countSeconds + ": " + contactSeconds.get(countSeconds));
								countSeconds = 0;
							} else {
								logger.debug("[EQ] " + countSeconds + ": " + contactSeconds.get(countSeconds));
							}
						}
						
						else
						
						if(i == tsArray.length - 1) {
							Integer currentTS = (Integer)tsArray[i];
							Integer prevTS = (Integer)tsArray[i-1];
							currentTS--;
							if(!currentTS.equals(prevTS)) {
								if(contactSeconds.containsKey(1)) {
									Integer curSec = contactSeconds.get(1);
									curSec++;
									contactSeconds.put(1, curSec);
								} else {
									contactSeconds.put(1, 1);
								}
								logger.debug("[INEQ, LAST] " + countSeconds + ": " + contactSeconds.get(countSeconds));
							} else {
								if(contactSeconds.containsKey(countSeconds)) {
									Integer curSec = contactSeconds.get(countSeconds);
									curSec++;
									contactSeconds.put(countSeconds, curSec);
								} else {
									contactSeconds.put(countSeconds, 1);
								}
								logger.debug("[EQ, LAST] " + countSeconds + ": " + contactSeconds.get(countSeconds));
							}
							countSeconds = 0;
						}
					}
				} else {
					if(contactSeconds.containsKey(1)){
						Integer curSec = contactSeconds.get(1);
						curSec++;
						contactSeconds.put(1, curSec);
					} else {
						contactSeconds.put(1, 1);
					}
					logger.debug("[UNIQUE] 1: " + contactSeconds.get(1));
				}
			}
			
			Map<Integer, Integer> contactSecondsSort = new TreeMap<Integer, Integer>(contactSeconds);
			Set<Map.Entry<Integer, Integer>> contactSecondsSet = contactSecondsSort.entrySet();
			for(Map.Entry<Integer, Integer> entry : contactSecondsSet) {
				logger.info(entry.getKey() + " seconds: " + entry.getValue() + " contacts");
			}
		}
	}

	public void writeFile(String outputFileName, String separator) throws IOException {
		printStats();
	}
	
	
	/** INNER CLASS */
	private class EdgeObject {
		private String source, target;
		
		private EdgeObject(String source, String target) {
			this.source = source;
			this.target = target;
		}
		
		@Override
	    public int hashCode() {
	        return source.hashCode() ^ target.hashCode();
	    }

	    @Override
	    public boolean equals(Object obj) {
	        return (obj instanceof EdgeObject) && ((EdgeObject) obj).source.equals(source)
	                                       && ((EdgeObject) obj).target.equals(target);
	    }

		@Override
		public String toString() {
			return "[" + source + "," + target + "]";
		}
	}
}
