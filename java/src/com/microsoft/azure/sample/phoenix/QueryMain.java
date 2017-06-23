package com.microsoft.azure.sample.phoenix;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class QueryMain {

	public static ResultSet[] querySequentially(String url, String[] queries) {
		ResultSet[] results = new ResultSet[queries.length];
		for (int i = 0; i < queries.length; i ++) {
			Connection conn = DBUtils.init_connection(url);
			ResultSet res = DBUtils.executeSingleQuery(conn, queries[i]);
			results[i] = res;
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
		return results;
	}
	
	public static void runFullTest(String[] servers, String[] queries, int concurrency, boolean output, boolean round_robin) {
		long t1,t2,t3 = 0;
		int split_size = queries.length/3;
		t1 = System.currentTimeMillis();
		ResultSet[] results1 = querySequentially(servers[0], Arrays.copyOfRange(queries, 0, split_size));
		t1 = System.currentTimeMillis() - t1;
		if (output) { for (ResultSet r : results1) { DBUtils.outputResult(r, 0);}}
		QueryThreadPool t_pool = new QueryThreadPool(concurrency, servers, round_robin);
		t2 = System.currentTimeMillis();
		ResultSet[] results2 = t_pool.executeQueries(Arrays.copyOfRange(queries, split_size, 2*split_size));
		t2 = System.currentTimeMillis() - t2;
		if (output) { for (ResultSet r : results2) { DBUtils.outputResult(r, 0);}}
		QueryConnectionPool c_pool = new QueryConnectionPool(concurrency, split_size, servers, round_robin);
		t3 = System.currentTimeMillis();
		ResultSet[] results3 = c_pool.executeQueries(Arrays.copyOfRange(queries, 2*split_size, 3*split_size));
		t3 = System.currentTimeMillis() - t3;
		if (output) { for (ResultSet r : results3) { DBUtils.outputResult(r, 0);}}
		System.out.println("Sequential: " + t1 + "ms");
		System.out.println("Thread Pool: " + t2 + "ms");
		System.out.println("Connection Pool: " + t3 + "ms");
	}
	
	public static void runTest(String[] servers, String[] queries, int mode, int concurrency, boolean output, boolean round_robin) {
		ResultSet[] results;
		long t;
		switch(mode) {
			case 1: {
				t = System.currentTimeMillis();
				results = querySequentially(servers[0], queries);
				t = System.currentTimeMillis() - t;
				if (output) { for (ResultSet r : results) { DBUtils.outputResult(r, 0);}}
				System.out.println("Sequential: " + t + "ms");
				break;
			}
			case 2: {
				QueryThreadPool t_pool = new QueryThreadPool(concurrency, servers, round_robin);
				t = System.currentTimeMillis();
				results = t_pool.executeQueries(queries);
				t = System.currentTimeMillis() - t;
				if (output) { for (ResultSet r : results) { DBUtils.outputResult(r, 0);}}
				System.out.println("Thread Pool: " + t + "ms");
				break;
			}
			case 3: {
				t = System.currentTimeMillis();
				QueryConnectionPool c_pool = new QueryConnectionPool(concurrency, queries.length, servers, round_robin);
				t = System.currentTimeMillis() - t;
				System.out.println("Connection init: " + t + "ms");
				t = System.currentTimeMillis();
				results = c_pool.executeQueries(queries);
				t = System.currentTimeMillis() - t;
				if (output) { for (ResultSet r : results) { DBUtils.outputResult(r, 0);}}
				System.out.println("Connection Pool: " + t + "ms");
				break;
			}
			default: {
				runFullTest(servers, queries, concurrency, output, round_robin);
				break;
			}
		}
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] servers = DBUtils.parseFile(args[0], "\n");
		for (int i = 0; i < servers.length; ++i) {
			servers[i] = String.format("jdbc:phoenix:thin:url=http://%s:8765;serialization=PROTOBUF", servers[i]);
		}
		String[] queries = DBUtils.parseFile(args[1], ";|\n");
		int concurrency = Integer.parseInt(args[2]);
		int mode = Integer.parseInt(args[3]);
		boolean output = Boolean.parseBoolean(args[4]);
		boolean round_robin = false;
		if (args.length >= 6) {
			round_robin = Boolean.parseBoolean(args[5]);
		}
		runTest(servers, queries, mode, concurrency, output, round_robin);
	}

}
