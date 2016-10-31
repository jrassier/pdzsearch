# pdzsearch
Parallel, distributed search within gzipped log files
## Usage overview
  **Index one or more files:** `pdzsearch.py index (year) (file1) [file2] [file3] [file4] ...`  
  Standard syslog output doesn't include a year, so you must specify it in order for the indexer to come up with the right timestamps. You'll have a Bad Time if you try to index a file with lines from multiple years, so be careful about Dec 31 and Jan 1 logs!  
  
  **Execute a search of all indexed files:** `pdzsearch.py search (pattern) (starttime) (endtime) (true|false)`  
  Where (starttime) and (endtime) are unix-epoch timestamps and true/false indicates whether or not to treat (pattern) as a regex. All matching lines are written to stdout.

## Examples
### A plain-text search
`pdzsearch.py search 'DHCPDISCOVER' 1477267200 1477872000 false`  
Returns all lines containing the string 'DHCPDISCOVER' between October 24, 2016 and October 31, 2016.  
### A regex search
`pdzsearch.py search 'DHCP(DISCOVER|OFFER|ACK)' 1477267200 1477872000 true`  
Returns all lines containing DHCPDISCOVER, DHCPOFFER, or DHCPACK in the same time frame.  

## Requirements
In no particular order, you'll need all this stuff to make my stuff work:  
- A redis or rabbitmq server that's network-accessible for the 'master' and all workers.
- A postgres server that's network-accessible for the 'master' and all workers.
- zlib
- zlib headers (zlib-dev or zlib-devel)
- a C compiler
- Python headers (python-dev or python-devel...you know the drill)
- re2 headers
- pip
- postgresql-client
- libpq headers
- The graf Python module (https://github.com/jrassier/graf)
- Recent versions of these pip packages:
amqp anyjson billiard celery kombu pylibmc pytz fb-re2 redis psycopg2

## Setup
Sorry, I hope to add more detail here soon...hopefully a look through the code and a read through the celery docs will fill in most of the gaps for now.
- Configure redis or rabbitmq properly for your environment.
- Stand up a postgres server or use an existing one.
- Run dbinit.sql to define the necessary tables.
- Configure pdzsearch.ini like the example.
- Distribute tasks.py and pdzsearch.py to any number of machines. All worker machines will, at the moment, need access to the whole body of logs that you want to operate on. This can be via NFS, replication to local storage, or some combination of the two (there's a rudimentary, incompletely-implemented mechanism by which workers can check a local path for a file before accessing it via the network)
- Index and search as described above.

## Known Issues
- Searches return incomplete lines if a line crosses an index-chunk boundary.
- Lines for which a matching string crosses a chunk boundary will not be returned.  
  
...in other words, indexing needs to be more newline-aware
