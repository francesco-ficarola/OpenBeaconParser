package it.uniroma1.dis.wsngroup.extensions.parsing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.swing.text.DateFormatter;

import cern.colt.matrix.impl.SparseDoubleMatrix2D;
import cern.colt.matrix.impl.SparseObjectMatrix2D;

import com.google.gson.Gson;
import it.uniroma1.dis.wsngroup.extensions.simulations.Node;
import it.uniroma1.dis.wsngroup.extensions.simulations.Utils;

/** 
 * This class takes as input a JSON file returned by TwitterSearch
 * and provides functionalities for creating either another JSON file representing the dynamic
 * network or a static representation of the network by means of a weighted adjacency graph
 */
public class TwitterJsonParser {
	private Gson gson;
	private File inputFile;
	private FileReader reader;
	private DateFormat formatter;
	
	public TwitterJsonParser(File f) throws FileNotFoundException {
		gson = new Gson();
		inputFile = f;
		reader = new FileReader(inputFile);
		formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
	}
	
	/** 
	 * Takes as input a JSON file returned by TwitterSearch and creates
	 * a new JSON file representing the evolution of the edges with
	 * respect to time (timestamps)
	 */
	public void createDynamicJsonFile() throws FileNotFoundException {
		BufferedReader br = new BufferedReader(reader);
		
		System.out.println("Parsing input JSON file...");
		Tweet[] tweets = gson.fromJson(br, Tweet[].class);
		long startingTimestamp = 0;
		
		try {
			startingTimestamp = (((Date)formatter.parse(tweets[tweets.length-1].getCreatedAt())).getTime())/1000;
		}
		catch (ParseException e) {
			System.out.println("Error: tweet " + tweets[tweets.length-1].getID() + " has a malformed creation date");
			System.exit(-1);
		}
		
		long timestep = 60; //seconds
		long currentTimestamp = startingTimestamp + timestep;
		long next = startingTimestamp + timestep;
		int end = tweets.length - 1;
		int id = 0;
		
		System.out.println("Creating output JSON file...");
		
		List<String> buffer = new LinkedList<String>();
		buffer.add("[");
		
		for (; true; currentTimestamp += timestep) {
			StringBuffer entry = new StringBuffer();
			
			if (id % 1000 == 0)
				System.out.println("* Current timestamp: " + currentTimestamp);
			
			if (id != 0) 
				entry.append(',');
			
			entry.append("{\"id\":" + (id++) + 
					",\"timestamp\":\"" + currentTimestamp + "\",");
			
			if (currentTimestamp < next) {
				entry.append("\"users\":[],\"edges\":[]}");
				buffer.add(entry.toString());
				continue;
			}
				
			LinkedList<Tweet> inTimestep = new LinkedList<Tweet>();
			for (;  end >= 0; end--) {
				try {
					long timestamp = -1;
					if (tweets[end] != null) {
						timestamp = (((Date)formatter.parse(tweets[end].getCreatedAt())).getTime())/1000;
						if (timestamp < currentTimestamp - timestep)
							timestamp = currentTimestamp - timestep;
					}
					else {
						System.out.println("Warning: null Tweet object encountered");
						continue;
					}

					if (timestamp <= currentTimestamp) {
						inTimestep.add(tweets[end]);
					}
					else {
						next = timestamp;
						break;
					}
				} catch (ParseException e) {
					System.out.println("Error: tweet " + tweets[end].getID() + " has a malformed creation date");
					System.exit(-1);
				}
			}
			
			entry.append("\"users\":[");
			
			int tempUsers = 0;
			for (Tweet t : inTimestep) {
				if (tempUsers++ != 0)
					entry.append(',');
				
				entry.append("{\"userid\":" + t.getFromUserID() + 
						",\"screen_name\":\"" + TwitterSearch.escape(t.getFromUser()) + 
						"\",\"name\":\"" + TwitterSearch.escape(t.getFromUserName()) + 
						"\"}");
				
				for (Mention m : t.getMentions()) {
					if (tempUsers++ != 0) 
						entry.append(',');
				
					entry.append("{\"userid\":" + m.getID() + 
							",\"screen_name\":\"" + TwitterSearch.escape(m.getScreenName()) + 
							"\",\"name\":\"" + TwitterSearch.escape(m.getName()) + 
							"\"}");
				}
			}
			
			entry.append("],\"edges\":[");
			
			int tempEdges = 0;
			for (Tweet t : inTimestep) {
				for (Mention m : t.getMentions()) {
					if (tempEdges++ != 0) 
						entry.append(',');
				
					entry.append("{\"source_target\":[" + t.getFromUserID() +
							"," + m.getID() + 
							"]}");
				}
			}
			
			entry.append("]}");
			buffer.add(entry.toString());
			
			if (end < 0)
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
	
	/**
	 * Takes as input a JSON file returned by TwitterSearch and creates
	 * a weighted adjacency graph representing the network
	 */
	public void createAggregatedAdjacencyGraph() throws FileNotFoundException {
		BufferedReader br = new BufferedReader(reader);
		HashMap<Long, User> map = new HashMap<Long, User>();
		
		Tweet[] tweets = gson.fromJson(br, Tweet[].class);
		
		for (int i = 0; i < tweets.length; i++) {
			Tweet tweet = tweets[i];
			User u = null;
			
			if (tweet == null) {
				System.out.println("Warning: null tweet encountered");
				continue;
			}
			
			if (!map.containsKey(tweet.getFromUserID())) {
				u = new User(tweet.getFromUserID(), tweet.getFromUser());
				map.put(tweet.getFromUserID(), u);
			}
			else u = map.get(tweet.getFromUserID());
				
			for (Mention m : tweet.getMentions()) {
				User um;
				
				if (!map.containsKey(m.getID())) {
					um = new User(m.getID(), m.getScreenName());
					map.put(m.getID(), um);
				}
				else um = map.get(m.getID());
				
				u.addLinkTo(um);
			}
		}
		
		ArrayList<User> users = new ArrayList<User>();
		
		for (Map.Entry<Long, User> entry : map.entrySet()) 
			users.add(entry.getValue());
		
		Collections.sort(users, new UserIDComparator());
		
		for (int i = 0; i < users.size(); i++)
			users.get(i).setIndex(i);
		
		int n = users.size();
		SparseDoubleMatrix2D adjMatrix = new SparseDoubleMatrix2D(n, n);
		adjMatrix.assign(0.0);
		
		for (User u : users) {
			int indexRow = u.getIndex();
			if (indexRow < 0) {
				System.out.println("Errore di indicizzazione al nodo " + u.getID());
				System.exit(-1);
			}
			
			for (User um : u.getLinksTo()) {
				int indexCol = um.getIndex();
				if (indexCol < 0) {
					System.out.println("Errore di indicizzazione al nodo" + u.getID());
					System.exit(-1);
				}
				
				adjMatrix.setQuick(indexRow, indexCol, adjMatrix.getQuick(indexRow, indexCol) + 1);
			}
		}
		
		StringBuffer sb = new StringBuffer();
		for (int j = 0; j < n; j++)
			sb.append(";" + users.get(j).getID() + "(" + users.get(j).getName() + ")");
		sb.append('\n');
		
		for (int i = 0; i < n; i++) {
			sb.append(users.get(i).getID() + "(" + users.get(i).getName() + ")");
			
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
	
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.print("Insert search JSON file: ");
		String input = br.readLine();
		File inputFile = new File(input);
		
		TwitterJsonParser tjp = new TwitterJsonParser(inputFile);
		
		tjp.createDynamicJsonFile();
//		tjp.createAggregatedAdjacencyGraph();
	}
	
	private class Tweet {
		private String created_at;
		private LinkedList<Hashtag> hashtags;
		private LinkedList<Mention> user_mentions;
		private String from_user;
		private long from_user_id;
		private String from_user_name;
		private long id;
		private String iso_language_code;
		private String text;
		private int to_user_id;

		public String getCreatedAt() {
			return created_at;
		}
		
		public LinkedList<Mention> getMentions() {
			return user_mentions;
		}
		
		public long getFromUserID() {
			return from_user_id;
		}
		
		public String getFromUser() {
			return from_user;
		}
		
		public String getFromUserName() {
			return from_user_name;
		}
		
		public long getID() {
			return id;
		}
		
		public String getText() {
			return text;
		}
		
	}
	
	private class Hashtag {
		private String text;
		int[] indices;
	}
	
	private class Mention {
		private String screen_name;
		private String name;
		private long id;
		private int[] indices;
		
		public long getID() {
			return id;
		}
		
		public String getScreenName() {
			return screen_name;
		}
		
		public String getName() {
			return name;
		}
	}
	
	private class User {
		private long id;
		private String name;
		private LinkedList<User> linksTo = new LinkedList<User>();
		private int index;
		
		public User(long i, String n) {
			id = i;
			name = n;
			index = -1;
		}
		
		public long getID() {
			return id;
		}
		
		public String getName() {
			return name;
		}
		
		public LinkedList<User> getLinksTo() {
			return linksTo;
		}
		
		public void addLinkTo(User u) {
			linksTo.add(u);
		}
		
		public int getIndex() {
			return index;
		}
		
		public void setIndex(int i) {
			index = i;
		}
	}
	
	private class UserIDComparator implements Comparator<User> {
		@Override
		public int compare(User u1, User u2) {
			if (u1.getID() == u2.getID()) return 0;
			if (u1.getID() < u2.getID()) return -1;
			return 1;
		}
	}
}
