package com.microsoft.azure.sample.phoenix;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.BlockingQueue;

public class QueryConnectionPool {
	private ExecutorService executor;
	private BlockingQueue<Connection> connections;
	private String[] servers;
	private boolean round_robin;
	
	public QueryConnectionPool(int concurrency, int num_connections, String[] servers, boolean round_robin) {
		this.executor = Executors.newFixedThreadPool(concurrency);
		connections = new LinkedBlockingQueue<Connection>(num_connections);
		this.servers = servers;
		this.round_robin = round_robin;
		for (int i = 0; i < num_connections; ++i) {
			connections.add(DBUtils.init_connection(getURL(i)));
		}
	}
	
	private String getURL(int idx) {
		if (round_robin) { 
			return servers[idx % servers.length];
		} else {
			return servers[0];
		}
	}
	
	
	@SuppressWarnings("finally")
	public ResultSet[] executeQueries(String[] queries) {
		ResultSet[] results = new ResultSet[queries.length];
		ReentrantLock lock = new ReentrantLock();
		for (int i = 0; i < queries.length; i ++) {
			QueryThread qt = new QueryThread(connections, queries[i], lock, results, i);
			executor.execute(qt);			
		}
		try {
			executor.shutdown();
			executor.awaitTermination(100, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		return results;
		}
	}
}
