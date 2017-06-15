from query_engine import QueryEngine
from timeit import default_timer as timer

queries = [
    "SELECT * from VIOLATIONS where STATE = 'MA' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'FL' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'PA' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'WA' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'CA' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'NY' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'NJ' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'AZ' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'OH' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'GA' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'VA' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'TX' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'MI' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'IL' LIMIT 30",
    "SELECT * from VIOLATIONS where STATE = 'NC' LIMIT 30"]
    
servers = [
"http://10.0.0.7:8765",
"http://10.0.0.6:8765",
"http://10.0.0.12:8765",
"http://10.0.0.13:8765"]

qe = QueryEngine(servers, 8, 15, round_robin=True, primary_server=1)

print 'Sequential Mode'
start = timer()
res1 = qe.sequential_mode(queries[11:15])
end = timer()
print end-start
print 'Threadpool Mode'
start = timer()
res2 = qe.threadpool_mode(queries[6:10])
end = timer()
print end-start
print 'Connection Pool Mode'
start = timer()
res3 = qe.connectionpool_mode(queries[0:4])
end = timer()
print end - start
