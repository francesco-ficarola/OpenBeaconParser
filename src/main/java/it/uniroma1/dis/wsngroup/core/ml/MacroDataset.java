package it.uniroma1.dis.wsngroup.core.ml;

import it.uniroma1.dis.wsngroup.core.Functions;
import it.uniroma1.dis.wsngroup.core.ml.Classification.TimestampObject;
import it.uniroma1.dis.wsngroup.core.ml.Classification.Visitor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
//import com.socialdis.core.MapUtil;

/**
 * @author Francesco Ficarola
 */

public class MacroDataset {
	
	private Logger logger = Logger.getLogger(this.getClass());
	
	private String separator;
	private File path;
	
	private int count0 = 0;
	private int count1 = 0;
	private int count2 = 0;
	
	/** Integer: PersonID, VisitorArtworkInstance (Visitor, totalTime) */
	private Map<Integer, VisitorArtworkInstance> mapArtwork_0xc0a85016;
	private Map<Integer, VisitorArtworkInstance> mapArtwork_0xc0a85009;
	private Map<Integer, VisitorArtworkInstance> mapArtwork_0xc0a8500d;
	private Map<Integer, VisitorArtworkInstance> mapArtwork_0xc0a85013;
	private Map<Integer, VisitorArtworkInstance> mapArtwork_0xc0a8501a;
	private Map<Integer, VisitorArtworkInstance> mapArtwork_0xc0a85017;
	private Map<Integer, VisitorArtworkInstance> mapArtwork_0xc0a85019;
	private Map<Integer, VisitorArtworkInstance> mapArtwork_0xc0a85012;
	
	private Map<String, Map<Integer, VisitorArtworkInstance>> mapsArtworkSet;
	
	private Map<Integer, VisitorArtworkInstance> sortedMapArtwork_0xc0a85016;
	private Map<Integer, VisitorArtworkInstance> sortedMapArtwork_0xc0a85009;
	private Map<Integer, VisitorArtworkInstance> sortedMapArtwork_0xc0a8500d;
	private Map<Integer, VisitorArtworkInstance> sortedMapArtwork_0xc0a85013;
	private Map<Integer, VisitorArtworkInstance> sortedMapArtwork_0xc0a8501a;
	private Map<Integer, VisitorArtworkInstance> sortedMapArtwork_0xc0a85017;
	private Map<Integer, VisitorArtworkInstance> sortedMapArtwork_0xc0a85019;
	private Map<Integer, VisitorArtworkInstance> sortedMapArtwork_0xc0a85012;
	
	private Map<String, Map<Integer, VisitorArtworkInstance>> sortedMapArtworkSet;
	
	private Map<String, InstanceProperties> propSet;
	
	public MacroDataset(String separator, File path) {
		this.separator = separator;
		this.path = path;
		
		mapArtwork_0xc0a85016 = new HashMap<Integer, VisitorArtworkInstance>();
		mapArtwork_0xc0a85009 = new HashMap<Integer, VisitorArtworkInstance>();
		mapArtwork_0xc0a8500d = new HashMap<Integer, VisitorArtworkInstance>();
		mapArtwork_0xc0a85013 = new HashMap<Integer, VisitorArtworkInstance>();
		mapArtwork_0xc0a8501a = new HashMap<Integer, VisitorArtworkInstance>();
		mapArtwork_0xc0a85017 = new HashMap<Integer, VisitorArtworkInstance>();
		mapArtwork_0xc0a85019 = new HashMap<Integer, VisitorArtworkInstance>();
		mapArtwork_0xc0a85012 = new HashMap<Integer, VisitorArtworkInstance>();
		
		mapsArtworkSet = new HashMap<String, Map<Integer, VisitorArtworkInstance>>();
		mapsArtworkSet.put("0xc0a85016", mapArtwork_0xc0a85016);
		mapsArtworkSet.put("0xc0a85009", mapArtwork_0xc0a85009);
		mapsArtworkSet.put("0xc0a8500d", mapArtwork_0xc0a8500d);
		mapsArtworkSet.put("0xc0a85013", mapArtwork_0xc0a85013);
		mapsArtworkSet.put("0xc0a8501a", mapArtwork_0xc0a8501a);
		mapsArtworkSet.put("0xc0a85017", mapArtwork_0xc0a85017);
		mapsArtworkSet.put("0xc0a85019", mapArtwork_0xc0a85019);
		mapsArtworkSet.put("0xc0a85012", mapArtwork_0xc0a85012);
		
		sortedMapArtworkSet = new HashMap<String, Map<Integer, VisitorArtworkInstance>>();
		
		propSet = new HashMap<String, InstanceProperties>();
		propSet.put("0xc0a85016", new InstanceProperties());
		propSet.put("0xc0a85009", new InstanceProperties());
		propSet.put("0xc0a8500d", new InstanceProperties());
		propSet.put("0xc0a85013", new InstanceProperties());
		propSet.put("0xc0a8501a", new InstanceProperties());
		propSet.put("0xc0a85017", new InstanceProperties());
		propSet.put("0xc0a85019", new InstanceProperties());
		propSet.put("0xc0a85012", new InstanceProperties());
	}

	public void buildDataset(List<TimestampObject> tsObjectList) {
		
		logger.info("Building Artwork HashMaps...");
		buildArtworkHashMaps(tsObjectList);
		
//		logger.info("Deleting noise...");
//		deleteNoise();
		
		logger.info("Sorting HashMaps by personid...");
		sortingHashMaps();
		
		logger.info("Computing mathematical properties...");
		computeProperties();
		
		logger.info("Classifying the total time for each visitor...");
		classifyTotalTime();
		
		logger.info("Writing Datasets...");
		try {
			writeDatasets();
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage(), e);
			//e.printStackTrace();
		}
	}
	
	
	private Map<Integer, VisitorArtworkInstance> getArtworkMap(String readerID) {
		Map<Integer, VisitorArtworkInstance> currentMap = null;
		
		if(mapsArtworkSet.containsKey(readerID)) {
			currentMap = mapsArtworkSet.get(readerID);
		}
		
		return currentMap;
	}
	
	
//	private void deleteNoise() {
//		
//		/** Get the set of all maps in mapsArtworkSet */
//		Set<Map.Entry<String, Map<Integer, VisitorArtworkInstance>>> allMapsArtwork = mapsArtworkSet.entrySet();
//				
//		for(Map.Entry<String, Map<Integer, VisitorArtworkInstance>> currentArtworkMapEntry : allMapsArtwork) {
//			String currentKey = currentArtworkMapEntry.getKey();
//			
//			List<Integer> instanceToDelete = new ArrayList<Integer>();
//			
//			/** Get every artwork map in the set */
//			Map<Integer, VisitorArtworkInstance> currentArtworkMap = mapsArtworkSet.get(currentKey);
//			
//			/** Get the set of all instances in currentArtworkMap */
//			Set<Map.Entry<Integer, VisitorArtworkInstance>> allInstances = currentArtworkMap.entrySet();
//			
//			for(Map.Entry<Integer, VisitorArtworkInstance> currentInstance : allInstances) {
//				Integer currentPersonId = currentInstance.getKey();
//				VisitorArtworkInstance currentVisitorArtworkInstance = currentArtworkMap.get(currentPersonId);
//				
//				Integer currentTotalTime = currentVisitorArtworkInstance.getObservationTotalTime();
//				
//				if(currentTotalTime <= 5 || currentTotalTime > 500) {
//					instanceToDelete.add(currentPersonId);
//				}
//			}
//			
//			for(int i = 0; i < instanceToDelete.size(); i++) {
//				currentArtworkMap.remove(instanceToDelete.get(i));
//			}
//		}
//	}
	
	
	private void computeProperties() {
		/** Get the set of all maps in mapsArtworkSet */
		Set<Map.Entry<String, Map<Integer, VisitorArtworkInstance>>> allMapsArtwork = mapsArtworkSet.entrySet();
				
		for(Map.Entry<String, Map<Integer, VisitorArtworkInstance>> currentArtworkMapEntry : allMapsArtwork) {
			String currentKey = currentArtworkMapEntry.getKey();
			
			List<Integer> totalTimeArray = new ArrayList<Integer>();
			
			/** Get every artwork map in the set */
			Map<Integer, VisitorArtworkInstance> currentArtworkMap = mapsArtworkSet.get(currentKey);
			
			/** Get the set of all instances in currentArtworkMap */
			Set<Map.Entry<Integer, VisitorArtworkInstance>> allInstances = currentArtworkMap.entrySet();
			
			for(Map.Entry<Integer, VisitorArtworkInstance> currentInstance : allInstances) {
				Integer currentPersonId = currentInstance.getKey();
				VisitorArtworkInstance currentVisitorArtworkInstance = currentArtworkMap.get(currentPersonId);
				
				Integer currentTotalTime = currentVisitorArtworkInstance.getObservationTotalTime();
				totalTimeArray.add(currentTotalTime);
			}
			
			InstanceProperties instanceProperties = propSet.get(currentKey);
			instanceProperties.setAvgTimeValue(Functions.average(totalTimeArray));
			instanceProperties.setMaxTimeValue(Functions.max(totalTimeArray));
			instanceProperties.setMinTimeValue(Functions.min(totalTimeArray));
		}
	}
	
	
	private void classifyTotalTime() {
		
		/** Get the set of all maps in mapsArtworkSet */
		Set<Map.Entry<String, Map<Integer, VisitorArtworkInstance>>> allMapsArtwork = mapsArtworkSet.entrySet();
		
		for(Map.Entry<String, Map<Integer, VisitorArtworkInstance>> currentArtworkMapEntry : allMapsArtwork) {
			String currentKeyNameArtwork = currentArtworkMapEntry.getKey();
			
			/** Get every artwork map in the set */
			Map<Integer, VisitorArtworkInstance> currentArtworkMap = mapsArtworkSet.get(currentKeyNameArtwork);
//			Integer instancesForGroup = currentArtworkMap.size() / 3;
//			Integer countInstances = 0;
			
			/** Get the set of all instances in currentArtworkMap */
			Set<Map.Entry<Integer, VisitorArtworkInstance>> allInstances = currentArtworkMap.entrySet();
			
			double avg = propSet.get(currentKeyNameArtwork).getAvgTimeValue();
			int min = propSet.get(currentKeyNameArtwork).getMinTimeValue();
			int max = propSet.get(currentKeyNameArtwork).getMaxTimeValue();
			
			double class_0_1 = avg * 0.3;
			double class_1_2 = avg;
			
			for(Map.Entry<Integer, VisitorArtworkInstance> currentInstance : allInstances) {
				Integer currentPersonId = currentInstance.getKey();
				VisitorArtworkInstance currentVisitorArtworkInstance = currentArtworkMap.get(currentPersonId);
				
				/** For each instance of type VisitorArtworkInstance in currentInstance get the total time */
				Integer classifiedTotalTime = -1;
				Integer currentTotalTime = currentVisitorArtworkInstance.getObservationTotalTime();
				
				logger.debug(currentVisitorArtworkInstance.getVisitor().getPersonid() + " - " + currentTotalTime);
				
				
				/** Three classes */
				if(currentTotalTime <= class_0_1) {
					classifiedTotalTime = 1;
					count0++;
				} else
				if(currentTotalTime > class_0_1 && currentTotalTime <= class_1_2) {
					classifiedTotalTime = 2;
					count1++;
				} else
				if(currentTotalTime > class_1_2) {
					classifiedTotalTime = 3;
					count2++;
				}
				
				/** Same number of instances for each group */
//				if(countInstances <= instancesForGroup) {
//					classifiedTotalTime = 0;
//					count0++;
//				} else
//				if(countInstances > instancesForGroup && countInstances <= (instancesForGroup * 2)) {
//					classifiedTotalTime = 1;
//					count1++;
//				} else {
//					classifiedTotalTime = 2;
//					count2++;
//				}
//				
//				countInstances++;
				
				/** Update the total time to the classified total time */
				currentVisitorArtworkInstance.setObservationTotalTime(classifiedTotalTime);
			}
			
			logger.info("*** Artwork " + currentKeyNameArtwork + " ***");
			logger.info("#visitors(class0): " + count0 + "    #visitors(class1): " + count1 + "    #visitors(class2): " + count2);
			logger.info("avg: " + avg + "    min: " + min + "    max: " + max);
			logger.info("(class0<" + class_0_1 + ")    (" + class_0_1 + "<class1<" + class_1_2 + ")    (class2>" + class_1_2 + ")");
			System.out.println();
			count0 = count1 = count2 = 0;
		}
		
	}
	
	
	private void buildArtworkHashMaps(List<TimestampObject> tsObjectList) {
		Iterator<TimestampObject> it = tsObjectList.iterator();
		
		while(it.hasNext()) {
			TimestampObject timestampObject = it.next();
			ArrayList<Visitor> visitors = timestampObject.getVisitors();
			
			logger.debug("##### " + timestampObject.getTimestamp() + " #####");
			
			if(visitors != null) {
				
				for(int i = 0; i < visitors.size(); i++) {
					
					Visitor currentVisitor = visitors.get(i);
					logger.debug("- visitor: " + currentVisitor.getPersonid());
					
					Integer personid = Integer.valueOf(currentVisitor.getPersonid());
					ArrayList<String> readerIDs = currentVisitor.getReaderIDs();
					
					if(readerIDs != null) {
						
						for(int j = 0; j < readerIDs.size(); j++) {
							Map<Integer, VisitorArtworkInstance> currentMap = getArtworkMap(readerIDs.get(j));
							if(currentMap != null) {
								
								/** Node already created */
								if(currentMap.containsKey(personid)) {
									Integer currentObservationTotalTime = currentMap.get(personid).getObservationTotalTime();
									currentMap.get(personid).setObservationTotalTime(currentObservationTotalTime + 1);
								}
								
								/** New node */
								else {
									VisitorArtworkInstance newVisitorArtworkInstance = new VisitorArtworkInstance(currentVisitor, 1);
									currentMap.put(personid, newVisitorArtworkInstance);
								}
							}
						}
						
					} else {
						logger.error("readerIDs array is null for the visitor " + personid);
					}
				}
				
			} else {
				logger.error("visitors array is null for the timestamp " + timestampObject.getTimestamp());
			}
			
			logger.debug("######################");
		}
	}
	
	
	private void sortingHashMaps() {
		// SORT BY PERSONID
		sortedMapArtwork_0xc0a85016 = new TreeMap<Integer, VisitorArtworkInstance>(mapArtwork_0xc0a85016);
		sortedMapArtwork_0xc0a85009 = new TreeMap<Integer, VisitorArtworkInstance>(mapArtwork_0xc0a85009);
		sortedMapArtwork_0xc0a8500d = new TreeMap<Integer, VisitorArtworkInstance>(mapArtwork_0xc0a8500d);
		sortedMapArtwork_0xc0a85013 = new TreeMap<Integer, VisitorArtworkInstance>(mapArtwork_0xc0a85013);
		sortedMapArtwork_0xc0a8501a = new TreeMap<Integer, VisitorArtworkInstance>(mapArtwork_0xc0a8501a);
		sortedMapArtwork_0xc0a85017 = new TreeMap<Integer, VisitorArtworkInstance>(mapArtwork_0xc0a85017);
		sortedMapArtwork_0xc0a85019 = new TreeMap<Integer, VisitorArtworkInstance>(mapArtwork_0xc0a85019);
		sortedMapArtwork_0xc0a85012 = new TreeMap<Integer, VisitorArtworkInstance>(mapArtwork_0xc0a85012);
		
		// SORT BY OBSERVATIONTOTALTIME
//		sortedMapArtwork_0xc0a85016 = MapUtil.sortByValue(mapArtwork_0xc0a85016);
//		sortedMapArtwork_0xc0a85009 = MapUtil.sortByValue(mapArtwork_0xc0a85009);
//		sortedMapArtwork_0xc0a8500d = MapUtil.sortByValue(mapArtwork_0xc0a8500d);
//		sortedMapArtwork_0xc0a85013 = MapUtil.sortByValue(mapArtwork_0xc0a85013);
//		sortedMapArtwork_0xc0a8501a = MapUtil.sortByValue(mapArtwork_0xc0a8501a);
//		sortedMapArtwork_0xc0a85017 = MapUtil.sortByValue(mapArtwork_0xc0a85017);
//		sortedMapArtwork_0xc0a85019 = MapUtil.sortByValue(mapArtwork_0xc0a85019);
//		sortedMapArtwork_0xc0a85012 = MapUtil.sortByValue(mapArtwork_0xc0a85012);
		
		sortedMapArtworkSet.put("0xc0a85016", sortedMapArtwork_0xc0a85016);
		sortedMapArtworkSet.put("0xc0a85009", sortedMapArtwork_0xc0a85009);
		sortedMapArtworkSet.put("0xc0a8500d", sortedMapArtwork_0xc0a8500d);
		sortedMapArtworkSet.put("0xc0a85013", sortedMapArtwork_0xc0a85013);
		sortedMapArtworkSet.put("0xc0a8501a", sortedMapArtwork_0xc0a8501a);
		sortedMapArtworkSet.put("0xc0a85017", sortedMapArtwork_0xc0a85017);
		sortedMapArtworkSet.put("0xc0a85019", sortedMapArtwork_0xc0a85019);
		sortedMapArtworkSet.put("0xc0a85012", sortedMapArtwork_0xc0a85012);
	}
	
	
	private void writeDatasets() throws FileNotFoundException {
		
		/** Get the set of all maps in mapsArtworkSet */
		Set<Map.Entry<String, Map<Integer, VisitorArtworkInstance>>> allMapsArtwork = sortedMapArtworkSet.entrySet();
		
		for(Map.Entry<String, Map<Integer, VisitorArtworkInstance>> currentArtworkMapEntry : allMapsArtwork) {
			String currentKey = currentArtworkMapEntry.getKey();
			
			File f = new File(path + "/" + "dataset_" + currentKey + ".csv");
			FileOutputStream fos = new FileOutputStream(f, false);
			PrintStream ps = new PrintStream(fos);
			
			/** Get every artwork map in the set */
			Map<Integer, VisitorArtworkInstance> currentArtworkMap = sortedMapArtworkSet.get(currentKey);
			
			/** Get the set of all instances in currentArtworkMap */
			Set<Map.Entry<Integer, VisitorArtworkInstance>> allInstances = currentArtworkMap.entrySet();
			
			for(Map.Entry<Integer, VisitorArtworkInstance> currentInstance : allInstances) {
				Integer currentPersonId = currentInstance.getKey();
				VisitorArtworkInstance currentVisitorArtworkInstance = currentArtworkMap.get(currentPersonId);
				
				Visitor visitor = currentVisitorArtworkInstance.getVisitor();
				Integer observationTotalTime = currentVisitorArtworkInstance.getObservationTotalTime();
				
				ps.println(
//						visitor.getPersonid() + separator +
						AttributeMapping.getSexValue(visitor.getSex()) + separator +
						visitor.getAge() + separator +
						AttributeMapping.getGradeLevelValue(visitor.getGrade()) + separator +
						AttributeMapping.getOccupationValue(visitor.getOccupation()) + separator +
//						AttributeMapping.getNationalityValue(visitor.getNationality()) + separator +
						observationTotalTime
						);
			}
			
			logger.info("Output file: " + f.getAbsolutePath());
			ps.close();
		}
	}
	
	
	
	/** INNER CLASS */
	
	private class VisitorArtworkInstance implements Comparable<VisitorArtworkInstance> {
		
		private Visitor visitor;
		private Integer observationTotalTime;

		public VisitorArtworkInstance(Visitor visitor, Integer totalTime) {
			this.visitor =  visitor;
			this.observationTotalTime = totalTime;
		}

		public Visitor getVisitor() {
			return visitor;
		}

//		public void setVisitor(Visitor visitor) {
//			this.visitor = visitor;
//		}

		public Integer getObservationTotalTime() {
			return observationTotalTime;
		}

		public void setObservationTotalTime(Integer totalTime) {
			this.observationTotalTime = totalTime;
		}

		public int compareTo(VisitorArtworkInstance n) {
			Integer personid1 = Integer.valueOf(visitor.getPersonid());
			Integer personid2 = Integer.valueOf(n.getVisitor().getPersonid());
			return personid1.compareTo(personid2);
		}
		
//		@Override
//		public int compareTo(VisitorArtworkInstance n) {
//			Integer totalTime1 = observationTotalTime;
//			Integer totalTime2 = n.getObservationTotalTime();
//			return totalTime1.compareTo(totalTime2);
//		}
	}
	
	
	private class InstanceProperties {
		private Integer minTimeValue;
		private Integer maxTimeValue;
		private Double avgTimeValue;
		
		public Integer getMinTimeValue() {
			return minTimeValue;
		}
		
		public void setMinTimeValue(Integer minTimeValue) {
			this.minTimeValue = minTimeValue;
		}
		
		public Integer getMaxTimeValue() {
			return maxTimeValue;
		}
		
		public void setMaxTimeValue(Integer maxTimeValue) {
			this.maxTimeValue = maxTimeValue;
		}
		
		public Double getAvgTimeValue() {
			return avgTimeValue;
		}
		
		public void setAvgTimeValue(Double avgTimeValue) {
			this.avgTimeValue = avgTimeValue;
		}
	}
}
