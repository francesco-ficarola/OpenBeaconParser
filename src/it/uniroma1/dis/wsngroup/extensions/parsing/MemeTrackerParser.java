package it.uniroma1.dis.wsngroup.extensions.parsing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import it.uniroma1.dis.wsngroup.extensions.simulations.Utils;

import cern.colt.matrix.impl.SparseDoubleMatrix2D;

/** 
 * This class takes as input a raw phrases data log download at www.memetracker.org
 * and provides functionalities for creating either a JSON file representing the dynamic
 * network associated to the log file or a static representation of this network by 
 * means of a weighted adjacency graph
 */
public class MemeTrackerParser {
	private File inputFile;
	private FileReader inputFileReader;
	private DateFormat formatter;
	
	public MemeTrackerParser(File f) throws FileNotFoundException {
		inputFile = f;
		inputFileReader = new FileReader(inputFile);
		formatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss", Locale.US);
	}
	
	public void createDynamicJsonFile() throws IOException, ParseException {		
		System.out.println("* Parsing record file...");
		Record[] records = parseRecordFile();
		
		long startingTimestamp = (((Date)formatter.parse(records[0].getTimestamp())).getTime())/1000;
		long timestep = 1; //seconds
		long currentTimestamp = startingTimestamp + timestep;
		long next = startingTimestamp + timestep; // next timestamp to be found
		int end = 0;
		int id = 0;
		
		List<String> buffer = new LinkedList<String>();
		buffer.add("[");
		
		System.out.println("* Creating JSON file...");		
		for (; true; currentTimestamp += timestep) {
			StringBuffer entry = new StringBuffer();
			
			if (id != 0) 
				entry.append(',');
			
			entry.append("{\"id\":" + (id++) + 
					",\"timestamp\":\"" + currentTimestamp + "\",");
			
			if (currentTimestamp < next) { //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				entry.append("\"documents\":[],\"edges\":[]}");
				buffer.add(entry.toString());
				continue;
			}
			
			LinkedList<Record> inTimestep = new LinkedList<Record>();
			for (;  end < records.length; end++) {
				try {
					long timestamp = -1;
					if (records[end] != null) {
						timestamp = (((Date)formatter.parse(records[end].getTimestamp())).getTime())/1000;
						if (timestamp < currentTimestamp - timestep)
							timestamp = currentTimestamp - timestep;
					}
					else {
						System.out.println("Warning: null Record object encountered");
						continue;
					}

					if (timestamp <= currentTimestamp) {
						inTimestep.add(records[end]);
					}
					else {
						next = timestamp;
						break;
					}
				} catch (ParseException e) {
					System.out.println("Error: a record has a malformed creation date: " + records[end].getTimestamp());
					System.exit(-1);
				}
			}
			
			entry.append("\"documents\":[");
			
			int tempUsers = 0;
			List<String> urlsInTimestep = new LinkedList<String>();
			for (Record r : inTimestep) {
				if (r.getHostURL().equals("twitter.com"))
					System.out.println("YES - " + currentTimestamp + " - outlinks=" + r.getOutLinks());
				if (!urlsInTimestep.contains(r.getHostURL())) {
					if (tempUsers++ != 0)
						entry.append(',');
					
					entry.append("{\"host_url_id\":" + getIDFromHostURL(r.getHostURL()) + 
							",\"host_url\":\"" + r.getHostURL() + 
							"\"}");
					
					urlsInTimestep.add(r.getHostURL());
				}
				
				for (String s : r.getOutLinks()) {
					if (!urlsInTimestep.contains(s)) {
						if (tempUsers++ != 0) 
							entry.append(',');
					
						entry.append("{\"host_url_id\":" + getIDFromHostURL(s) +
								",\"host_url\":\"" + s + 
								"\"}");
						
						urlsInTimestep.add(s);
					}
				}
			}
			
			entry.append("],\"edges\":[");
			
			int tempEdges = 0;
			for (Record r : inTimestep) {
				for (String s : r.getOutLinks()) {
					if (tempEdges++ != 0) 
						entry.append(',');
				
					entry.append("{\"source_target\":[" + getIDFromHostURL(r.getHostURL()) +
							"," + getIDFromHostURL(s) + 
							"]}");
					
					if (getIDFromHostURL(r.getHostURL()) == getIDFromHostURL(s))
							System.out.println("!!");
				}
			}
			
			entry.append("]}");
			buffer.add(entry.toString());
			
			if (end >= records.length)
				break;
		}
		
		buffer.add("]");
		
		PrintStream ps = new PrintStream(new File(inputFile.getParentFile().getAbsolutePath() +
				"/dynamic_graph.json"));
		
		for (String s : buffer)
			ps.print(s);
		
		ps.close();
		
		System.out.println("Done!");
	}
	
	public Record[] parseRecordFile() throws IOException, ParseException {
		BufferedReader br = new BufferedReader(inputFileReader);
		
		Set<String> outLinks = new HashSet<String>();
		Set<String> nodeSet = new HashSet<String>();
		List<Record> records = new LinkedList<Record>();
		
		String line;
		URL documentURL = null;
		String timestamp = null;
		long first = 1000000000;
		
		int countRecords = 0;
		while ((line = escape(br.readLine())) != null) {					
			if (line.length() > 0 && line.charAt(0) == 'P') { // P = URL of the document
				String urlString = line.substring(line.indexOf('\t') + 1);
				documentURL = parseURLString(urlString);
			}
			else if (line.length() > 0 && line.charAt(0) == 'T') { // T = timestamp of the document
				timestamp = line.substring(line.indexOf('\t') + 1);
				
				long now = (((Date)formatter.parse(timestamp)).getTime())/1000;
				if (countRecords == 0) {
					first = now;
				} else {
					int days = 1; // how many days to take into account in the simulation?
					if ((now - first) > (60 * 60 * 24 * days))
						break;
				}
			}
			else if (line.length() > 0 && line.charAt(0) == 'L') { // L = link to another document
				String linkURLString = line.substring(line.indexOf('\t') + 1);
				URL linkURL = parseURLString(linkURLString);

				if (linkURL != null){
						outLinks.add(linkURL.getHost());
				}
			}
			else if (line.equals("")) { // end of record
				if (documentURL != null && timestamp != null && outLinks.size() > 0) {
					Record r = new Record(documentURL.toString(), documentURL.getHost(), timestamp, outLinks);
					records.add(r);
					nodeSet.add(documentURL.getHost());
				}
				
				documentURL = null;
				timestamp = null;
				outLinks.clear();
				countRecords++;
			}
		}
			
		List<Record> cleanRecords = new LinkedList<Record>();
		for (Record r : records) {
			List<String> toRemove = new LinkedList<String>();
		
			for (String ol : r.getOutLinks()) {
				if (!nodeSet.contains(ol))
					toRemove.add(ol);
			}
			
			for (String l : toRemove) {
				r.removeOutLink(l);
			}
			
			if (r.getOutLinks().size() > 0)
				cleanRecords.add(r);
		}

		return cleanRecords.toArray(new Record[cleanRecords.size()]);
	}
	
	public void createAggregatedAdjacencyGraph() throws IOException, ParseException {
		HashMap<Integer, Document> map = new HashMap<Integer, Document>();
		
		System.out.println("Parsing record file...");
		Record[] records = parseRecordFile();
		
		System.out.println("Creating aggregated adjacency graph...");
		for (int i = 0; i < records.length; i++) {
			Record record = records[i];
			Document d = null;
			
			if (record == null) {
				System.out.println("Warning: null record encountered");
				continue;
			}
			
			if (!map.containsKey(getIDFromHostURL(record.getHostURL()))) {
				d = new Document(getIDFromHostURL(record.getHostURL()), record.getHostURL());
				map.put(getIDFromHostURL(record.getHostURL()), d);
			}
			else d = map.get(getIDFromHostURL(record.getHostURL()));
				
			for (String s : record.getOutLinks()) {
				Document doc = null;
				
				if (!map.containsKey(getIDFromHostURL(s))) {
					doc = new Document(getIDFromHostURL(s), s);
					map.put(getIDFromHostURL(s), doc);
				}
				else doc = map.get(getIDFromHostURL(s));
				
				d.addLinkTo(doc);
			}
		}
		
		ArrayList<Document> documents = new ArrayList<Document>();
		
		for (Map.Entry<Integer, Document> entry : map.entrySet()) 
			documents.add(entry.getValue());
		
		Collections.sort(documents, new DocumentIDComparator());
		
		for (int i = 0; i < documents.size(); i++)
			documents.get(i).setIndex(i);
		
		int n = documents.size();
		SparseDoubleMatrix2D adjMatrix = new SparseDoubleMatrix2D(n, n);
		adjMatrix.assign(0.0);
		
		for (Document d : documents) {
			int indexRow = d.getIndex();
			if (indexRow < 0) {
				System.out.println("Errore di indicizzazione al nodo " + d.getHostURLid());
				System.exit(-1);
			}
			
			for (Document doc : d.getLinksTo()) {
				int indexCol = doc.getIndex();
				if (indexCol < 0) {
					System.out.println("Errore di indicizzazione al nodo" + d.getHostURLid());
					System.exit(-1);
				}
				
				adjMatrix.setQuick(indexRow, indexCol, adjMatrix.getQuick(indexRow, indexCol) + 1);
			}
		}
		
		StringBuffer sb = new StringBuffer();
		for (int j = 0; j < n; j++)
			sb.append(";" + documents.get(j).getHostURLid() + "(" + documents.get(j).getHostURL() + ")");
		sb.append('\n');
		
		for (int i = 0; i < n; i++) {
			sb.append(documents.get(i).getHostURLid() + "(" + documents.get(i).getHostURL() + ")");
			
			for (int j = 0; j < n; j++)
				sb.append(";" + (int)adjMatrix.getQuick(i, j));
			
			sb.append('\n');
		}	
		
		Utils.printToCSVFile(sb.toString(), inputFile.getParentFile().getAbsolutePath() +
				"/adjacency_matrix.csv"); 
		
		System.out.println("Done!");
	}	
	
	public static String concatenateStrings(List<String> items) {
        if (items == null)
            return null;
        if (items.size() == 0)
            return "";
        
        int expectedSize = 0;
        for (String item: items)
            expectedSize += item.length();
        
        StringBuffer result = new StringBuffer(expectedSize);
        for (String item: items)
            result.append(item);
        
        return result.toString();
    }
	
	private URL parseURLString(String urlString) {
		try {
			URL aURL = new URL(urlString);
	        return aURL;
		} catch (MalformedURLException e) {
			try {
				int ind = urlString.indexOf("http:", 1);
				URL aURL = null;
				if (ind > -1)
					aURL = new URL(urlString.substring(ind));
				else 
					aURL = new URL(urlString);
				
				return aURL;
			} catch (MalformedURLException e2) {
				System.out.println("Error, malformed url: " + urlString);
				return null;
			}
		}
	}
	
	private String escape(String s) {
		if (s == null)
			return null;
		
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < s.length(); i++)
			if (s.charAt(i) != '\\')
				sb.append(s.charAt(i));
		
		return sb.toString();
	}
	
	public int getIDFromHostURL(String hostURL) {
		return hostURL.hashCode();
	}
	
	public static void main(String[] args) throws IOException, ParseException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.print("Insert input raw phrases file: ");
		File inputFile = new File(br.readLine());
		
		if (!inputFile.exists()) {
			System.out.println("ERROR: invalid file");
			System.exit(-1);
		}
		
		MemeTrackerParser mtp = new MemeTrackerParser(inputFile);
		
		mtp.createDynamicJsonFile();
//		mtp.createAggregatedAdjacencyGraph();
		
	}
	
	private class Record {
		private String documentURL;
		private String hostURL;
		private String timestamp;
		private List<String> outLinks;
		
		public Record(String du, String hu) {
			documentURL = du;
			hostURL = hu;
			timestamp = null;
			outLinks = new LinkedList<String>();
		}
		
		public Record(String du, String hu, String t, Set<String> l) {
			documentURL = du;
			hostURL = hu;
			timestamp = t;
			outLinks = new LinkedList<String>();
			outLinks.addAll(l);
			outLinks.remove(hostURL);
		}
		
		public String getDocumentURL() {
			return documentURL;
		}
		
		public String getHostURL() {
			return hostURL;
		}
		
		public String getTimestamp() {
			return timestamp;
		}
		
		public void setTimestamp(String t) {
			timestamp = t;
		}
		
		public List<String> getOutLinks() {
			return outLinks;
		}
		
		public void addOutLink(String l) {
			if (!outLinks.contains(l))
				outLinks.add(l);
		}
		
		public void removeOutLink(String l) {
			if (outLinks.contains(l))
				outLinks.remove(l);
		}
		
		
		
		public String toString() {
			return "Document URL = " + documentURL + "\nHost URL = " +
					hostURL + "\nTimestamp = " + timestamp + "\n#Outlinks = " + outLinks.size();
		}
	}
	
	private class Document {
		private long hostURLid;
		private String hostURL;
		private LinkedList<Document> linksTo = new LinkedList<Document>();
		private int index;
		
		public Document(long i, String u) {
			hostURLid = i;
			hostURL = u;
			index = -1;
		}
		
		public long getHostURLid() {
			return hostURLid;
		}
		
		public String getHostURL() {
			return hostURL;
		}
		
		public LinkedList<Document> getLinksTo() {
			return linksTo;
		}
		
		public void addLinkTo(Document d) {
			linksTo.add(d);
		}
		
		public int getIndex() {
			return index;
		}
		
		public void setIndex(int i) {
			index = i;
		}
	}
	
	private class DocumentIDComparator implements Comparator<Document> {
		@Override
		public int compare(Document d1, Document d2) {
			if (d1.getHostURLid() == d2.getHostURLid()) return 0;
			if (d1.getHostURLid() < d2.getHostURLid()) return -1;
			return 1;
		}
	}
}
