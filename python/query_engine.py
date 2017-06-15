from Queue import Queue
from threading import Lock
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from multiprocessing.dummy import Pool as ThreadPool
import phoenixdb
import sys


class QueryEngine:

  def __init__(self, db_urls, concurrency, num_connections, round_robin=False, primary_server=0):
    """
    db_urls:         []str, Phoenix query servers to be used
    concurrency:     Int, number of threads
    num_connections: Int, connection pool size
    round_robin:     Bool, determine if using multiple servers
    primary_server:  int, if only one server is used which one
    """
    self.db_urls = db_urls
    self.pool = ThreadPool(concurrency)
    self.connections = []
    self.num_connections = num_connections
    self.round_robin = round_robin
    self.primary_server = primary_server
    for i in range(num_connections):
      db_url = db_urls[primary_server] if not round_robin \
                 else db_urls[i%len(db_urls)]
      new_conn = phoenixdb.connect(db_url)
      cursor = new_conn.cursor()
      self.connections.append((cursor, Lock()))
      
  def conn_and_query(self, (idx, sql)):
    """
    Create a connection to Phoenix query server and execute the query
    Params:
    (idx:int, sql:str)
    idx: index of query server url in self.db_urls
    sql: query statement to be executed
    """
    db_url = self.db_urls[self.primary_server] if not self.round_robin \
               else self.db_urls[idx%len(self.db_urls)]
    db = phoenixdb.connect(db_url)
    cursor = db.cursor()
    cursor.execute(sql)
    res = cursor.fetchall()
    cursor.close()
    db.close()
    return res

  def get_conn_and_query(self, (idx, sql)):
    """
    Get a connection from the connection pool and execute the query
    Params: (idx:int, sql:str)
    idx: index of connection in the connection pool
    sql: query statement to be executed
    """
    self.connections[idx%self.num_connections][1].acquire()
    cursor = self.connections[idx%self.num_connections][0]
    cursor.execute(sql)
    res = cursor.fetchall()
    self.connections[idx%self.num_connections][1].release()
    return res
    
  def sequential_mode(self, queries):
    """
    Run the queries sequentially
    """
    results = []
    for query in queries:
      conn = phoenixdb.connect(self.db_urls[0])
      cursor = conn.cursor()
      cursor.execute(query)
      results.append(cursor.fetchall())
      conn.close()
    return results

  def threadpool_mode(self, queries):
    """
    Run the queries in thread pool mode
    """
    results = self.pool.map(self.conn_and_query, enumerate(queries))
    return results

  def connectionpool_mode(self, queries):
    """
    Run the queries with pre-initialized connections
    """
    results = self.pool.map(self.get_conn_and_query, enumerate(queries))
    return results
