package main

import "fmt"
import "time"
import  "gw_phoenix"

func main() {
  queries := []string{
    "SELECT COUNT(*) FROM VIOLATIONS WHERE STATE='MA'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE STATE='WA'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE STATE='FL'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE STATE='CA'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE STATE='NY'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE STATE='NJ'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE STATE='PA'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE STATE='TX'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE VIOLATION='speeding'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE VIOLATION='DWI'",
    "SELECT COUNT(*) FROM VIOLATIONS WHERE VIOLATION='DUI'"}
 
  q_engine,_ := gw_phoenix.NewQueryEngine("http://10.0.0.12:8765", 15, 25)
  var start time.Time
  var elapsed time.Duration
  fmt.Println("Sequential Mode Start")
  start = time.Now()
  results,_ := q_engine.QuerySequential(queries, true)
  elapsed = time.Since(start)
  gw_phoenix.OutputResults(results)
  fmt.Println(elapsed)
  fmt.Println("Thread pool Mode Start")
  start = time.Now()
  results = q_engine.QueryThreadPoolMode(queries, true)
  gw_phoenix.OutputResults(results)
  elapsed = time.Since(start)
  fmt.Println(elapsed)
  fmt.Println("Connection Pool Mode Start")
  start = time.Now()
  results = q_engine.QueryConnectionPoolMode(queries, true)
  elapsed = time.Since(start)
  gw_phoenix.OutputResults(results)
  fmt.Println(elapsed)
}
