package it.uniroma1.dis.wsngroup.extensions.parsing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import it.uniroma1.dis.wsngroup.extensions.simulations.Utils;

import twitter4j.HashtagEntity;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.UserMentionEntity;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/** 
 * Class for collecting @reply Twitter data through the search API.
 * The credentials at the and of the page must be filled in. 
 */
public class TwitterSearch {
	private ConfigurationBuilder builder;
	private Configuration conf;
    private Twitter twitter;
	private long maxID;
	
	public TwitterSearch() {
		maxID = 0;
		builder = new ConfigurationBuilder();
		builder.setOAuthConsumerKey(Constants.CONSUMER_KEY);
		builder.setOAuthConsumerSecret(Constants.CONSUMER_SECRET);
		builder.setOAuthAccessToken(Constants.ACCESS_TOKEN);
		builder.setOAuthAccessTokenSecret(Constants.ACCESS_TOKEN_SECRET);
		conf = builder.build();
		twitter = new TwitterFactory(conf).getInstance();
	}
	
	public String search(String q, String until, int page, long max_id) {
		 try {
        	Query query = new Query();
        	query.setResultType("recent");
        	query.setRpp(100);
        	
        	if (q != null && !q.equals("")) {
        		query.setQuery(q);
        	}
        	else {
        		System.out.println("Error, you must enter a valid query");
        		System.exit(-1);
        	}
        	
        	if (until != null && !until.equals("")) 
        		query.setUntil(until);
        	
        	if (max_id != 0)
        		query.setMaxId(max_id);
        	
        	QueryResult result = twitter.search(query);
            List<Tweet> tweets = result.getTweets();
            
            String output = "";
            int count = 0;
            
            Tweet lastTweet = tweets.get(tweets.size() - 1);
            maxID = lastTweet.getId();
            tweets.remove(tweets.size() - 1);
            System.out.println("Last tweet of page " + page + " - date: " + lastTweet.getCreatedAt() 
            		+ ", max id: " + maxID);
            
            if (tweets.size() == 0)
            	return "";
            
            for (Tweet tweet : tweets) {
            	UserMentionEntity[] mentions = tweet.getUserMentionEntities();
            	
            	if (mentions.length == 0) // we store only tweets with at least one @reply 
            		continue;
            	
            	if (count > 0) output += ",";
            	count++;
            	
            	HashtagEntity[] hashtags = tweet.getHashtagEntities();
                output += "{" +
                		"\"created_at\":\"" + tweet.getCreatedAt() + 
                		"\",\"hashtags\":" +
                		"[";
                
                for (int i = 0; i < hashtags.length; i++) {
                	if (i > 0) output += ",";
                	
                	output += "{" +
                			"\"text\":\"" + escape(hashtags[i].getText()) + "\",\"indices\":" +
                				"[" + 
                				hashtags[i].getStart() + "," + hashtags[i].getEnd() +
                				"]" +
                				"}";
                }
                
                output += "]" +
                		",\"user_mentions\":" +
                		"[";
                
                for (int i = 0; i < mentions.length; i++) {
                	if (i > 0) output += ",";
                	
                	output += "{" +
                			"\"screen_name\":\"" + mentions[i].getScreenName() + 
                			"\",\"name\":\"" + mentions[i].getName() + 
                			"\",\"id\":" + mentions[i].getId() + 
                			",\"indices\":" +
                			"[" + mentions[i].getStart() + "," + 
                			mentions[i].getEnd() + 
                			"]" +
                			"}";
                }
                
                output += "]" +
                	",\"from_user\":\"" + tweet.getFromUser() + 
                	"\",\"from_user_id\":" + tweet.getFromUserId() + 
                	",\"from_user_name\":\"" + tweet.getFromUserName() + 
                	"\",\"id\":" + tweet.getId() + 
                	",\"iso_language_code\":\"" + tweet.getIsoLanguageCode() +
                	"\",\"text\":\"" + escape(tweet.getText()) + 
                	"\",\"to_user\":\"" + tweet.getToUser() + 
                	"\",\"to_user_id\":" + tweet.getToUserId() + 
                	",\"to_user_name\":\"" + tweet.getToUserName() +
                	"\",\"in_reply_to_status_id\":" + tweet.getInReplyToStatusId() +
                	"}";
            }
            
            return output;
		 
		 } catch (TwitterException te) {
			 te.printStackTrace();
			 System.out.println("Failed to search tweets in page " + page + ": " + te.getMessage());
			 return null;
		 }
	}
	
	public long getMaxID() {
		return maxID;
	}
	
	public void setMaxID(long id) {
		maxID = id;
	}
	
	public static String escape(String s) {
		if(s == null)
			return null;
		StringBuffer sb = new StringBuffer();
		escape(s, sb);
		return sb.toString();
	}

	static void escape(String s, StringBuffer sb) {
		for(int i = 0;i < s.length(); i++){
			char ch = s.charAt(i);
			switch(ch) {
				case '"':
					sb.append("\\\"");
					break;
				case '\\':
					sb.append("\\\\");
					break;
				case '\b':
					sb.append("\\b");
					break;
				case '\f':
					sb.append("\\f");
					break;
				case '\n':
					sb.append("\\n");
					break;
				case '\r':
					sb.append("\\r");
					break;
				case '\t':
					sb.append("\\t");
					break;
				case '/':
					sb.append("\\/");
					break;
				default:
					if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F') || 
							(ch >= '\u2000' && ch <= '\u20FF')) {
						String ss = Integer.toHexString(ch);
						sb.append("\\u");
						for(int k = 0; k < 4-ss.length(); k++) {
							sb.append('0');
						}
						sb.append(ss.toUpperCase());
					}
					else {
						sb.append(ch);
					}
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		if (Constants.ACCESS_TOKEN.equals("") || Constants.ACCESS_TOKEN_SECRET.equals("") ||
				Constants.CONSUMER_KEY.equals("") || Constants.CONSUMER_SECRET.equals("")) {
			System.out.println("Error, the Twitter API credentials (ACCESS_TOKEN, ACCESS_TOKEN_SECRET, " +
					"CONSUMER_KEY, CONSUMER_SECRET) must be filled in");
			System.exit(-1);
		}
		
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		TwitterSearch ts = new TwitterSearch();
		
		System.out.print("Insert output file path: ");
		String outputFileName = br.readLine();
		System.out.print("Insert search query: ");
		String query = br.readLine();
		System.out.print("Insert 'until' date formatted as YYYY-MM-DD (optional): ");
		String until = br.readLine();
		System.out.print("Insert starting page (optional, default=1): ");
		String pageString = br.readLine();
		System.out.print("Insert max_id (optional): ");
		String max_id = br.readLine();
		
		Long l;
		if (max_id != null && !max_id.equals("")) {
			l = new Long(max_id);
		}
		else l = new Long(0);
		ts.setMaxID(l);
		
		int page;
		if (Utils.isInt(pageString))
			page = Integer.parseInt(pageString);
		else if (pageString.equals(""))
			page = 1;
		else {
			System.out.println("Invalid page, default (1) will be applied");
			page = 1;
		}
        
		System.out.println("* Starting search...");
		
		while(true) {
			String res;
			if ((res = ts.search(query, until, page, ts.getMaxID())) != null) {
				FileWriter fileWritter = new FileWriter(outputFileName, true);
				BufferedWriter bw = new BufferedWriter(fileWritter);
				
				if (page == 1) 
					bw.write("[");
				else bw.write(",");
				
				if (res.equals("")) {
					bw.write("]");
					bw.close();
					System.out.println("* End of search");
					break;
				}
				
				if (page > 1) 
					bw.write(",");
				
				bw.write(res);
				bw.close();
				
				System.out.println("* Wrote page " + page + " on file");
				page++;
			}
			else break;
		}
	}
	
	/** Credentials for Twitter API access */
	private class Constants {
	    public static final String CONSUMER_KEY = "";
	    public static final String CONSUMER_SECRET= "";
	    public static final String ACCESS_TOKEN = "";
	    public static final String ACCESS_TOKEN_SECRET= "";
	}
}
