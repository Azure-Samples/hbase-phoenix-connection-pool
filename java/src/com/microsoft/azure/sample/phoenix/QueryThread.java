package com.microsoft.azure.sample.phoenix;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class QueryThread implements Runnable {
	private Connection conn;
	private BlockingQueue<Connection> connections;
	private String URL;
	private String sql;
	private ReentrantLock lock;
	private ResultSet[] results;
	private int idx;
	
	public QueryThread(BlockingQueue<Connection> connections, String sql, 
			ReentrantLock lock, ResultSet[] results, int idx) {
		this.connections = connections;
		this.lock = lock;
		this.sql = sql;
		this.results = results;
		this.idx = idx;
	}
	
	public QueryThread(String URL, String sql, 
			ReentrantLock lock, ResultSet[] results, int idx) {
		this.URL = URL;
		this.sql = sql;
		this.lock = lock;
		this.results = results;
		this.idx = idx;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		if (connections !=  null) {
			conn = connections.poll();
		} else {
			conn = DBUtils.init_connection(URL);
		}
		ResultSet res = DBUtils.executeSingleQuery(conn, sql);
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		lock.lock();
		results[idx] = res;
		lock.unlock();
	}

}
