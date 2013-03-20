package it.uniroma1.dis.wsngroup.core.ml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import weka.classifiers.bayes.NaiveBayes;

import net.sf.javaml.classification.Classifier;
import net.sf.javaml.classification.evaluation.CrossValidation;
import net.sf.javaml.classification.evaluation.PerformanceMeasure;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.Instance;
import net.sf.javaml.tools.data.FileHandler;
import net.sf.javaml.tools.weka.WekaClassifier;

public class NaiveBayesClassificator {
	
	private Logger logger = Logger.getLogger(this.getClass());
	
	private File pathDatasets;
	private boolean outcomes;
	private Map<String, Dataset> datasetMap;

	public NaiveBayesClassificator(File pathDatasets, boolean outcomes) {
		this.pathDatasets = pathDatasets;
		this.outcomes = outcomes;
		datasetMap = new HashMap<String, Dataset>();
	}

	public void importDatasets() {
		
		try {
			
			/* Load data */
			Dataset dataset_0xc0a85016 = FileHandler.loadDataset(new File(pathDatasets + "/dataset_0xc0a85016.csv"), 4, ",");
			Dataset dataset_0xc0a85009 = FileHandler.loadDataset(new File(pathDatasets + "/dataset_0xc0a85009.csv"), 4, ",");
			Dataset dataset_0xc0a8500d = FileHandler.loadDataset(new File(pathDatasets + "/dataset_0xc0a8500d.csv"), 4, ",");
			Dataset dataset_0xc0a85013 = FileHandler.loadDataset(new File(pathDatasets + "/dataset_0xc0a85013.csv"), 4, ",");
			Dataset dataset_0xc0a8501a = FileHandler.loadDataset(new File(pathDatasets + "/dataset_0xc0a8501a.csv"), 4, ",");
			Dataset dataset_0xc0a85017 = FileHandler.loadDataset(new File(pathDatasets + "/dataset_0xc0a85017.csv"), 4, ",");
			Dataset dataset_0xc0a85019 = FileHandler.loadDataset(new File(pathDatasets + "/dataset_0xc0a85019.csv"), 4, ",");
			Dataset dataset_0xc0a85012 = FileHandler.loadDataset(new File(pathDatasets + "/dataset_0xc0a85012.csv"), 4, ",");
			
			/* Store dataset in the datasetMap */
			datasetMap.put("0xc0a85016", dataset_0xc0a85016);
			datasetMap.put("0xc0a85009", dataset_0xc0a85009);
			datasetMap.put("0xc0a8500d", dataset_0xc0a8500d);
			datasetMap.put("0xc0a85013", dataset_0xc0a85013);
			datasetMap.put("0xc0a8501a", dataset_0xc0a8501a);
			datasetMap.put("0xc0a85017", dataset_0xc0a85017);
			datasetMap.put("0xc0a85019", dataset_0xc0a85019);
			datasetMap.put("0xc0a85012", dataset_0xc0a85012);
			
			/* Create Weka classifier */
	        NaiveBayes naiveBayes = new NaiveBayes();
	        
	        /* Wrap Weka classifier in bridge */
	        Classifier bridgeWeka = new WekaClassifier(naiveBayes);
	        
	        Set<Map.Entry<String, Dataset>> allDatasets = datasetMap.entrySet();
	        for(Map.Entry<String, Dataset> datasetEntry : allDatasets) {
	        	String currentKeyArtwork = datasetEntry.getKey();
	        	Dataset currentDataset = datasetMap.get(currentKeyArtwork);
	        	
	        	logger.info("*** Dataset: " + "dataset_" + currentKeyArtwork + ".csv ***");
	        	
	        	/* Build classifier */
	        	bridgeWeka.buildClassifier(currentDataset);
	        	
	        	 /* Initialize cross-validation */
		        logger.info("Cross validation...");
		        CrossValidation cv = new CrossValidation(bridgeWeka);
	        	
	        	/* Perform cross-validation */
	        	Map<Object, PerformanceMeasure> pm = cv.crossValidation(currentDataset);
	        	
	        	/* Output results */
		        for (Object o : pm.keySet()) {
		        	logger.info("Accuray " + o + " = " + pm.get(o).getAccuracy() + "   " +
		        				"Error Rate " + o + " = " + pm.get(o).getErrorRate());
		        }
		        
		        logger.info(pm);
		               
		        
		        if(outcomes) {
		        	
		        	logger.info("Outlining the outcomes for " + currentKeyArtwork + "...");
		        	
		        	File f_real = new File(pathDatasets + "/" + "RealClassValues_" + currentKeyArtwork + ".csv");
		        	FileOutputStream fos_real = new FileOutputStream(f_real, false);
					PrintStream ps_real = new PrintStream(fos_real);
		        	
		        	File f_pred = new File(pathDatasets + "/" + "PredClassValues_" + currentKeyArtwork + ".csv");
		        	FileOutputStream fos_pred = new FileOutputStream(f_pred, false);
					PrintStream ps_pred = new PrintStream(fos_pred);
		        	
		        	Dataset dataForClassification = FileHandler.loadDataset(new File(pathDatasets + "/dataset_" + currentKeyArtwork + ".csv"), 4, ",");
			        /* Counters for correct and wrong predictions. */
			        int correct = 0, wrong = 0;
			        /* Classify all instances and check with the correct class values */
			        int countVisitors = 1;
			        for (Instance inst : dataForClassification) {
			        	Object realClassValue = inst.classValue();
			        	Object predictedClassValue = bridgeWeka.classify(inst);
			            if (predictedClassValue.equals(realClassValue)) {
			                correct++;
//			            	System.out.println("CORRECT: " + inst + " " + predictedClassValue);
			            }
			            else {
			            	wrong++;
//			            	System.out.println("WRONG: " + inst + " " + predictedClassValue);
			            }
			            
			            ps_real.print(countVisitors + ",");
			            ps_pred.print(countVisitors + ",");
			            
			            for(int i = 0; i < inst.values().size(); i++) {
			            	ps_real.print((int)inst.value(i) + ",");
							ps_pred.print((int)inst.value(i) + ",");
						}
			            
			            ps_real.println(realClassValue);
						ps_pred.println(predictedClassValue);
						
						countVisitors++;
			        }
			        
			        logger.info("Correct predictions  " + correct);
			        logger.info("Wrong predictions " + wrong);
			        
			        ps_real.close();
			        ps_pred.close();
		        }
		        
		        System.out.println();
	        }	              
			
	        
		} catch (IOException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		}
	}
}
