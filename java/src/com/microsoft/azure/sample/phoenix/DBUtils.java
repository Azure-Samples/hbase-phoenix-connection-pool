package com.microsoft.azure.sample.phoenix;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtils {
	
	
	
	public static String[] parseFile(String filePath, String delim) {
		try {
			String content = new String(Files.readAllBytes(Paths.get(filePath)));
			return content.split(delim);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public static Connection init_connection(String url) {
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			return DriverManager.getConnection(url);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public static ResultSet executeSingleQuery(Connection conn, String sql) {
		try {
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.executeQuery();
			return stmt.getResultSet();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
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
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	}
}
