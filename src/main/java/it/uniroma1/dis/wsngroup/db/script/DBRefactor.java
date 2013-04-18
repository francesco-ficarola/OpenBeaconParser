package it.uniroma1.dis.wsngroup.db.script;

import it.uniroma1.dis.wsngroup.constants.DBConstants;
import it.uniroma1.dis.wsngroup.db.DBObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;


public class DBRefactor {

	/**
	 * @author Francesco Ficarola
	 *
	 */
	
	private static Logger logger = Logger.getLogger(DBRefactor.class);
	
	public static void main(String[] args) {
		DBObject db = new DBObject(DBConstants.DB_TYPE, DBConstants.DB_SERVER_HOSTNAME, DBConstants.DB_SERVER_PORT, DBConstants.DIS_DB_NAME, DBConstants.DIS_DB_USERNAME, DBConstants.DIS_DB_PASSWORD);
		Connection connection = null;
		try {
			connection = db.getConnection();			
			logger.info("Connected to database " + DBConstants.DB_SERVER_HOSTNAME);
			
			refactorTable(connection, "Tag", DBConstants.DIS_TAG_TABLE_NAME);
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		} finally {
			if (connection != null) {
		    	  try {
					connection.close();
					logger.info("Disonnected to database " + DBConstants.DB_SERVER_HOSTNAME);
				} catch (SQLException e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}
	    }
	}
	
	private static void refactorTable(Connection c, String sourceTable, String destTable) {
		
		try {
			Statement stmtCreate = c.createStatement();
			String queryCreate = "CREATE TABLE IF NOT EXISTS " + destTable + " (TagID INTEGER NOT NULL PRIMARY KEY, Gender VARCHAR(1) NOT NULL, Age INTEGER NOT NULL, Course TEXT NOT NULL, Year INTEGER) CHARACTER SET utf8 COLLATE utf8_general_ci";
			stmtCreate.executeUpdate(queryCreate);
			logger.info("Table " + destTable + " created.");
			stmtCreate.close();
			
			Statement stmtSelect = c.createStatement();
			String querySelect = "SELECT * FROM " + sourceTable;
			ResultSet rsSelect = stmtSelect.executeQuery(querySelect);
			
			while (rsSelect.next()) {
				int tagID = rsSelect.getInt("TagID");
				String gender = rsSelect.getString("Sex");
				int age = rsSelect.getInt("Age"); 
				String status = rsSelect.getString("Status");
				
				logger.debug(tagID + "\t" + gender + "\t" + age + "\t" + status);
				
				StringTokenizer st = new StringTokenizer(status, "_");
				ArrayList<String> tokens = new ArrayList<String>();
				while(st.hasMoreTokens()) {
					tokens.add(st.nextToken());
				}
				
				Statement stmtInsert = c.createStatement();
				
				if(tokens.size() == 1) {
					String course = tokens.get(0);
					String queryInsert = "INSERT INTO " + destTable + " (TagID, Gender, Age, Course) VALUES(" + tagID + ", " + "'" + gender + "'" + ", " + age + ", " + "'" + course + "'" + ")";
					logger.debug(queryInsert);
					stmtInsert.executeUpdate(queryInsert);
				} else
				if(tokens.size() == 2) {
					int year = 0;
					if(tokens.get(1).equalsIgnoreCase("PrimoAnno")) {
						year = 1;
					} else
					if(tokens.get(1).equalsIgnoreCase("SecondoAnno")) {
						year = 2;
					} else
					if(tokens.get(1).equalsIgnoreCase("TerzoAnno")) {
						year = 3;
					} else
					if(tokens.get(1).equalsIgnoreCase("QuartoAnno")) {
						year = 4;
					} else
					if(tokens.get(1).equalsIgnoreCase("QuintoAnno")) {
						year = 5;
					} else {
						logger.error("Incorrect year for TagID " + tagID);
					}
					
					String course = tokens.get(0);
					String queryInsert = "INSERT INTO " + destTable + " (TagID, Gender, Age, Course, Year) VALUES(" + tagID + ", " + "'" + gender + "'" + ", " + age + ", " + "'" + course + "'" + ", " + year + ")";
					logger.debug(queryInsert);
					stmtInsert.executeUpdate(queryInsert);
					
				} else {
					logger.error("Status tokens less than 1 for TagID " + tagID);
				}
				
				stmtInsert.close();
			}
			
			stmtSelect.close();
			
		} catch(SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
}
