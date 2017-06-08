from threading import Thread,Lock
from Queue import Queue
import multiprocessing as mp
#from multiprocessing.pool import ThreadPool
from multiprocessing.dummy import Pool as ThreadPool
from timeit import default_timer as timer
import phoenixdb
import sys

class QueryEngine:

  def __init__(self, db_urls, concurrency, num_connections, round_robin=False, primary_server=0):
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
    self.connections[idx%self.num_connections][1].acquire()
    cursor = self.connections[idx%self.num_connections][0]
    cursor.execute(sql)
    res = cursor.fetchall()
    self.connections[idx%self.num_connections][1].release()
    return res


  def sequential_mode(self, queries):
    results = []
    for query in queries:
      conn = phoenixdb.connect(self.db_urls[0])
      cursor = conn.cursor()
      cursor.execute(query)
      results.append(cursor.fetchall())
      conn.close()
    return results

  def threadpool_mode(self, queries):
    results = self.pool.map(self.conn_and_query, enumerate(queries))
    return results

  def connectionpool_mode(self, queries):
    results = self.pool.map(self.get_conn_and_query, enumerate(queries))
    return results
