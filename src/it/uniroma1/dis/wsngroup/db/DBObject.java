package it.uniroma1.dis.wsngroup.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBObject {
	
	/**
	 * @author Francesco Ficarola
	 *
	 */
		
	private String db_type;
	private String db_serverHostname;
	private String db_serverPort;
	private String db_name;
	private String db_username;
	private String db_password;
	
	public DBObject(String db_type, String db_serverHostname, String db_serverPort, String db_name, String db_username, String db_password) {
		this.db_type = db_type;
		this.db_serverHostname = db_serverHostname;
		this.db_serverPort = db_serverPort;
		this.db_name = db_name;
		this.db_username = db_username;
		this.db_password = db_password;
	}
	
	public Connection getConnection() throws SQLException {
		Connection connection = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", db_username);
		connectionProps.put("password", db_password);
		
		connection = DriverManager.getConnection("jdbc:" + db_type + "://" + db_serverHostname + ":" + db_serverPort + "/" + db_name, connectionProps);
		return connection;
	}
}
