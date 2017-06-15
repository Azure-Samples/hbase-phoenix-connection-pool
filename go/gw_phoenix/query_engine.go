package gw_phoenix

import "fmt"
import "sync"
import "database/sql"
import _ "github.com/Boostport/avatica"


// Apache phoenix query engine supporting multiple query modes
type QueryEngine struct {
  server       string
  concurrency  int
  connections  []*sql.DB
}

type Result interface{}

type QueryRes struct {
  Res Result
  Err error
}

// Create a new query engine
// Connection pool is prepared
func NewQueryEngine (server string, concurrency int, num_connections int) (*QueryEngine, error){
  db_connections := make([]*sql.DB, num_connections)
  for i := 0; i < num_connections; i ++ {
    new_conn, err := sql.Open("avatica", server)
    if err == nil {
      db_connections[i] = new_conn
    } else {
      return nil,err
    }
  }
  new_engine := QueryEngine {
    server      : server,
    concurrency : concurrency,
    connections : db_connections,
  }
  return &new_engine,nil
}


// Execute a query
func (this *QueryEngine) exec_query (db_conn *sql.DB, query string, single bool) *QueryRes {
  if db_conn == nil {
    var err error
    db_conn, err = sql.Open("avatica", this.server)
    if err != nil {
      return nil
    }
  }
  if single {
    res := db_conn.QueryRow(query)
    return &QueryRes{Res : Result(res), Err : nil,}
  } else {
    res,err := db_conn.Query(query)
    return &QueryRes {Res : Result(res), Err : err,}
  }
}

func (this *QueryEngine) threadsToInit (num_queries int) int {
  if num_queries < this.concurrency {
    return num_queries
  } else {
    return this.concurrency
  }
}

// Execute the queries using sequential mode
func (this *QueryEngine) QuerySequential(queries []string, single bool) ([]*QueryRes, error){
  db, err := sql.Open("avatica", this.server)
  if err != nil {
    return nil,err
  } else {
    results := make([]*QueryRes, len(queries))
    for i, q := range queries {
      results[i] = this.exec_query(db, q, single)
    }
    return results,nil
  }
}

// Execute the queries in thread pool mode.
// Each query is executed in a thread.
func (this *QueryEngine) QueryThreadPoolMode(queries []string, single bool) []*QueryRes{
  results := make([]*QueryRes, len(queries))
  q_chan := make(chan int)
  var wg sync.WaitGroup
  for i := 0; i < this.threadsToInit(len(queries)); i++ {
    go func(){
      for q_idx := range q_chan {
        db_conn,err := sql.Open("avatica", this.server)
        if err != nil {
          results[q_idx] = &QueryRes{Res : nil, Err : err,}
        } else {
          results[q_idx] = this.exec_query(nil, queries[q_idx], single)
        }
        db_conn.Close()
        wg.Done()
      }
    }()
  }
  wg.Add(len(queries))
  for idx,_ := range queries {
    q_chan <- idx;
  }
  close(q_chan)
  wg.Wait()
  //close(q_chan)
  return results
}

// Execute the queries using the pre-initialized connections
// in the connection pool.
func (this *QueryEngine) QueryConnectionPoolMode (queries []string, single bool) []*QueryRes {
  results := make([]*QueryRes, len(queries))
  q_chan := make(chan int)
  c_chan := make(chan int, len(this.connections))
  var wg sync.WaitGroup
  for c_idx := 0; c_idx < len(this.connections); c_idx ++ {
    c_chan <- c_idx
  }

  for i := 0; i < this.threadsToInit(len(queries)); i++ {
    go func(){
      for q_idx := range q_chan {
        c_idx := <-c_chan
        results[q_idx] = this.exec_query(this.connections[c_idx], queries[q_idx], single)
        wg.Done()
        c_chan <- c_idx
      }
    }()
  }
  wg.Add(len(queries))
  for q_idx,_ := range queries {
    q_chan <- q_idx
  }
  close(q_chan)
  wg.Wait()
  //close(q_chan)
  close(c_chan)
  return results
}

func OutputResults(results []*QueryRes) {
  var value string
  for idx, q_res := range results {
    fmt.Printf("Query %v: ", idx)
    switch res := q_res.Res.(type) {
      case *sql.Row:
        if err := res.Scan(&value); err != nil {
          fmt.Println("Error")
        } else {
          fmt.Println(value)
        }
      case *sql.Rows:
        fmt.Println()
        for res.Next() {
          if err := res.Scan(&value); err != nil {
            fmt.Println("Error")
          } else {
            fmt.Println(value)
          }
        }
    }
  }
}
