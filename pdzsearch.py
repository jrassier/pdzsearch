#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  


from datetime import datetime
import sqlite3
import graf
from tasks import cat,search,isearch,regexsearch,buildindex,grafsearch
import time
import uuid
import psycopg2
import ConfigParser
import sys

cfg = ConfigParser.RawConfigParser()
cfg.read('pdzsearch.ini')
dbname = cfg.get('Database','dbname')
dbhost = cfg.get('Database','host')
dbuser = cfg.get('Database','user')
dbpass = cfg.get('Database','password')
spansize = cfg.get('Index','spansize')

def indexFiles(filelist, year, idxdb):
	# This is the list of Celery tasks that we make
	reslist = []

	#Enqueue one job for each file
	for filename in filelist:
		res = buildindex.delay(filename,(filename + '.idx'),spansize,year)
		reslist.append(res)
		print "Queued task %s to index file %s" % (res.id, filename)

	# Poll the stack of jobs until they're all complete or failed
	starttime = time.time()
	got_res = 0
	while(len(reslist) > 0):
		print "%d tasks outstanding, %d sec elapsed" % (len(reslist), time.time() - starttime)
		for r in reslist:
			if(r.ready()):
				print "Task %s complete." % (r.id)
				reslist.remove(r)
				got_res = 1
			elif(r.failed()):
				print "Task %s failed for some reason." % r.id
				reslist.remove(r)
				got_res = 1
		if(got_res == 1):
			got_res = 0
			next
		else:
			time.sleep(1)

def determineSearchChunks(starttime, endtime):
	# Figure out which chunks occur between the provided start time and end time
	chunks = []
	
	conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbhost)
	cur = conn.cursor()
	chunkSQL = ('SELECT file.id, file.name, chunk.uoffset, chunk.length '
				'FROM file, chunk '
				'WHERE file.id = chunk.file_id '
				"AND chunk.begin >= COALESCE((SELECT MAX(chunk.begin) FROM chunk WHERE chunk.begin <= to_timestamp(%s) at time zone 'UTC'),to_timestamp(0) at time zone 'UTC') "
				"AND (chunk.begin < to_timestamp(%s) at time zone 'UTC')"
			   )

	cur.execute(chunkSQL, (starttime, endtime))

	for row in cur.fetchall():
		chunks.append((int(row[0]), row[1], int(row[2]), int(row[3])))
	conn.close()
	return chunks
	
def doSearch(searchterm, starttime, endtime, is_regex):
	jobcode = str(uuid.uuid4())
	tasklist = []

	chunklist = determineSearchChunks(starttime, endtime)
	
	#print "Preparing search job %s" % jobcode
	
	for chunk in chunklist:
		
		#def grafsearch(jobcode, file_id, in_gzfile, in_idxfile, startoffset, readlen, searchterm):
		
		task = grafsearch.delay(jobcode, chunk[0],chunk[1], (chunk[1] + '.idx'),chunk[2], chunk[3], searchterm, is_regex)
		tasklist.append(task)
		#print "  Job %s: Queued %s for file %s, offset %s, len %s" % (jobcode, task.id, chunk[1], chunk[2], chunk[3])

	starttime = time.time()
	got_res = 0
	while(len(tasklist) > 0):
		#print "%d task(s) outstanding, %d sec elapsed" % (len(tasklist), (time.time() - starttime))
		for t in tasklist:
			if(t.ready()):
				#print "Task %s complete." % (t.id)
				tasklist.remove(t)
				got_res = 1
			elif(t.failed()):
				#print "Task %s failed." % (t.id)
				tasklist.remove(t)
				got_res = 1
		if(got_res == 1):
			got_res = 0
			next
		else:
			time.sleep(1)
	
	conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbhost)
	cur = conn.cursor()
	
	cur.execute("SELECT line FROM result WHERE jobcode = %s ORDER BY chunk_uoffset ASC, linenum ASC;", (jobcode,))
	for row in cur:
		print row[0]
	conn.close()

is_regex = False

def usage():
	print "pdzsearch: Parallel, distributed search within gzipped log files"
	print "Usage:"
	print ""
	print "1. Index: "
	print "   %s index (file1) [file2] [file3] [file4] ..." % sys.argv[0]
	print ""
	print "2. Search:"
	print "   %s search (pattern) (starttime) (endtime) (true|false)" % sys.argv[0]
	print "   where the final true/false argument determines whether (pattern)"
	print "   will be evaluated as a regex (true) or plain text (false)"
	print ""
	print "Results will be returned on stdout."
	print ""
	print "This is alpha software, and still mostly a proof of concept. Expect bugs!"
	print "Fixes and other contributions welcome at https://github.com/jrassier/pdzsearch"
	print ""

if(len(sys.argv) > 1):
	if(sys.argv[1] == 'index'):
		print "Index something"
	elif(sys.argv[1] == 'search'):
		if(len(sys.argv) > 5):
			if(sys.argv[5].lower() in ("yes", "true", "t", "y", "1")):
				is_regex = True
		doSearch(sys.argv[2], sys.argv[3], sys.argv[4], is_regex)
	else:
		usage()
else:
	usage()


