package com.microsoft.azure.sample.phoenix;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.sql.ResultSet;

/*
 * Class for simple thread pool executing multiple queries
 */
public class QueryThreadPool {
	private ExecutorService executor;
	private String[] servers;
	private boolean round_robin;
	
	/*
	 * Create a thread pool of query threads
	 * Each thread creates a connection, execute the query and close the connection
	 * @param concurrency - size of thread pool
	 * @param servers     - phoenix query servers in the cluster
	 * @param round-robin - whether to select from servers in Round-Robin mode
	 */
	public QueryThreadPool(int concurrency, String[] servers, boolean round_robin) {
		this.executor = Executors.newFixedThreadPool(concurrency);
		this.servers = servers;
		this.round_robin = round_robin;
	}
	
	/*
	 * Get the URL string for connection
	 */
	private String getURL(int idx) {
		if (round_robin) { 
			return servers[idx % servers.length];
		} else {
			return servers[0];
		}
	}
	
	/*
	 * Execute the queries concurrently
	 */
	@SuppressWarnings("finally")
	public ResultSet[] executeQueries(String[] queries) {
		ResultSet[] results = new ResultSet[queries.length];
		ReentrantLock lock = new ReentrantLock();
		for (int i = 0; i < queries.length; i ++) {
			QueryThread qt = new QueryThread(getURL(i), queries[i], lock, results, i);
			executor.execute(qt);
		}
		try {
			executor.shutdown();
			executor.awaitTermination(100, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			return results;
		}
	}
}
