package com.microsoft.azure.sample.phoenix;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/*
 * Class for a thread which executes a query
 */
public class QueryThread implements Runnable {
	private Connection conn;
	private BlockingQueue<Connection> connections;
	private String connStr;
	private String sql;
	private ReentrantLock lock;
	private ResultSet[] results;
	private int idx;
	
	/*
	 * Create a thread which uses a connection from a connection pool
	 * @param connections - a blocking queue which stores the pre-initialized connections
	 * @param sql         - query statement to be executed
	 * @param lock        - for synchronization use when writing results
	 * @param results     - destination array of execution results
	 * @param idx         - index in the destination array
	 */
	public QueryThread(BlockingQueue<Connection> connections, String sql, 
			ReentrantLock lock, ResultSet[] results, int idx) {
		this.connections = connections;
		this.lock = lock;
		this.sql = sql;
		this.results = results;
		this.idx = idx;
	}
	
	/*
	 * Create a thread which inits a new connection
	 * @param connStr - connection string pointing to a phoenix query server
	 * @param sql     - query statement to be executed
	 * @param lock    - for synchronization use when writing results
	 * @param results - destination array of execution results
	 * @param idx     - index in the destination array
	 */
	public QueryThread(String connStr, String sql, 
			ReentrantLock lock, ResultSet[] results, int idx) {
		this.connStr = connStr;
		this.sql = sql;
		this.lock = lock;
		this.results = results;
		this.idx = idx;
	}
	@Override
	public void run() {
		if (connections !=  null) {
			conn = connections.poll();
		} else {
			conn = DBUtils.init_connection(connStr);
		}
		ResultSet res = DBUtils.executeSingleQuery(conn, sql);
		try {
			conn.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		lock.lock();
		results[idx] = res;
		lock.unlock();
	}

}
