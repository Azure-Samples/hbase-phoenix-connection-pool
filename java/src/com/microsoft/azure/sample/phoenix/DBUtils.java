package com.microsoft.azure.sample.phoenix;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/*
 * Utilities for phoenix connection and query execution
 */
public class DBUtils {
	
	/*
	 * Parse a text file by splitting into string[].
	 * Mainly used for parsing file of hosts or of queries
	 */
	public static String[] parseFile(String filePath, String delim) {
		try {
			String content = new String(Files.readAllBytes(Paths.get(filePath)));
			return content.split(delim);
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	/*
	 * Create a connection to a phoenix query server
	 * @param connStr - connection string to phoenix query server
	 */
	public static Connection init_connection(String connStr) {
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			return DriverManager.getConnection(connStr);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			return null;
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	/*
	 * Execute a single query given the connection
	 */
	public static ResultSet executeSingleQuery(Connection conn, String sql) {
		try {
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.executeQuery();
			return stmt.getResultSet();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	/*
	 * Output the result of a query.
	 */
	public static void outputResult(ResultSet res, int limit) {
		try {
			int columnsNumber = res.getMetaData().getColumnCount();
			while (res.next()) {
				for (int i = 1; i <= columnsNumber; i++) {
					if (i > 1) System.out.print(",  ");
					String columnValue = res.getString(i);
					System.out.print(columnValue);
				}
				System.out.println("");
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
