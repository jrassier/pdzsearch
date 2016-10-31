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

import time
import re2 as re2
from celery import Celery
from datetime import datetime
import graf
import random
import ConfigParser
import psycopg2
import os

cfg = ConfigParser.RawConfigParser()
cfg.read('pdzsearch.ini')
dbname = cfg.get('Database','dbname')
dbhost = cfg.get('Database','host')
dbuser = cfg.get('Database','user')
dbpass = cfg.get('Database','password')

cbroker = cfg.get('Celery','broker')
cbackend = cfg.get('Celery','backend')

localdir = cfg.get('Dirs','local')
remotedir = cfg.get('Dirs','remote')

app = Celery('tasks', broker=cbroker, backend=cbackend)

app.conf.CELERY_ACKS_LATE = True
app.conf.CELERYD_PREFETCH_MULTIPLIER = 1
app.conf.CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']

@app.task
def cat(file):
    results = []
    i = 0
    with open(file,'r') as f:
        for line in f:
            results.append("{} {}".format(i,line.rstrip('\n')))
        i += 1
    return results

@app.task
def search(file, substr):
    results = []
    i = 0
    with open(file,'r') as f:
        for line in f:
            if substr in line:
                results.append("{} {}".format(i,line.rstrip('\n')))
            i += 1
    return results

@app.task
def isearch(file, substr):
    results = []
    i = 0
    with open(file,'r') as f:
        for line in f:
            if substr.lower() in line.lower():
                results.append("{} {}".format(i,line.rstrip('\n')))
            i += 1
    return results

@app.task
def regexsearch(file, regex):
    pat = re2.compile(regex)
    results = []
    i = 0
    with open(file,'r') as f:
        for line in f:
            if pat.search(line):
                results.append("{} {}".format(i,line.rstrip('\n')))
            i += 1
    return results

@app.task
def buildindex(in_gzfile, idxfile, spansize, year):

	# Given a gzip file, destination index file, span size and year,
	# generate a zran-format idx file and insert rows of format
	# (uncompressed offset, span length, start epoch) into the shared
	# database.
	
	# gzfile: The gzip file we're indexing
	# idxfile: The zran-format index file to write
	# spansize: Uncompressed length of each span
	# year: We need to pass this in because our log files don't contain the year.
	# Note that net.log-yyyy0101.gz will need special handling as it will
	# contain lines from two different years. This is left to the caller to handle properly.
	
	# We'll read 1024 bytes at a time looking for a newline
	chunksize = 1024

	gzfile = resolve_filename(in_gzfile)
	
	if(gzfile == ""):
		return "Unable to resolve gzfile"

	# Initialize database connection and cursor
	conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbhost)
	cur = conn.cursor()

	filename = os.path.basename(gzfile)

	#print "Creating file record for file %s" % filename

	# Create a file record
	cur.execute("INSERT INTO file (name) VALUES (%s) RETURNING id;",(filename,))
	fileid = cur.fetchone()[0]
	
	#print "Created file record %d for file %s" % (fileid, filename)

	# Make a zran index file
	res = graf.build_index(gzfile, idxfile, spansize)
	if(res != 0):
		return "Index build failed"

	# Now loop through the spans in that index and get the timestamp at
	# which each of them begins:

	spans = graf.read_index_offsets(idxfile)
	# Tuples of format (i, (uoffset, coffset, bits))

	fileusize = graf.get_usize(idxfile)

	for (i, span) in enumerate(spans):
		#print("------------------------------------------")
		#print("Attempting to timestamp span %d from uoffset %d:" % (i,span[0]))
		spandata = ""
		nlidx = -1
		msgday = ""
		msgtime = ""
		nchunks = 0
		
		while(nlidx == -1):
			#print("    Reading %d bytes:" % chunksize)
			#print("    --------------------")
			spandata = graf.extract(gzfile, idxfile, (span[0] + (chunksize * nchunks)), chunksize)
			if(span[0] == 0):
				spandata = "\n" + spandata    # We know the file starts with a timestamp, so fudge in a newline to fool subsequent logic. Hacky much?
			#print("    %s" % spandata)
			nlidx = spandata.find("\n")		# find returns -1 if the substring isn't found. This is a kinda-sorta magic number situation.
			#if(nlidx == -1): print("    Nope, no newline there.")
			# Make sure we have at least 15 bytes after the newline or we're going to get a bogus timestamp. Another magic number.
			if(nlidx > (chunksize - 15)):
				nlidx = -1
			nchunks += 1
			#print("    --------------------")
			
		msgday = spandata[nlidx+1:nlidx+7] + " " + str(year)
		msgtime = spandata[nlidx+8:nlidx+16]
		
		timestr = (msgday + " " + msgtime).replace("  ", " 0")

		#stamp = time.strptime(timestr, "%b %d %Y %H:%M:%S")
		#epoch = int(time.mktime(stamp))

		epoch = int(time.mktime(time.strptime(timestr, "%b %d %Y %H:%M:%S")))

		# While there are still more spans, we can just subtract the start offset of this span from
		# the start offset of the next span to get the length of this span.
		spanlen = 0
		if(len(spans) > i+1):
			nextspan = spans[i+1]
			spanlen = nextspan[0] - span[0]
		else:
			# When we reach the last span, we can get its length by subtracting its start offset
			# from the uncompressed file size.
			spanlen = fileusize - span[0]

		#print("Result: [Span %d] [Timestring {%s}] [Epoch stamp %s] [Span length %d]" % (i,timestr,str(epoch),spanlen))

		cur.execute("INSERT INTO chunk (file_id, uoffset, length, begin) VALUES (%s, %s, %s, to_timestamp(%s)::timestamp without time zone) RETURNING id;",(fileid, span[0], spanlen, epoch))
		chunkid = cur.fetchone()[0]

	conn.commit()
	conn.close()
	result = "Indexed %d chunks" % (i + 1)
	return result

@app.task
def grafsearch(jobcode, file_id, in_gzfile, in_idxfile, startoffset, readlen, searchterm, is_regex):
	
	gzfile = resolve_filename(in_gzfile)
	idxfile = resolve_filename(in_idxfile)
	
	if(gzfile == "" or idxfile == ""):
		return "Unable to resolve gzfile or index file"
	
	conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbhost)
	cur = conn.cursor()
	
	#print "Search file %s using idxfile %s from start offset %d for %d bytes with searchterm %s" % (gzfile, idxfile, startoffset, readlen, searchterm)
	data = graf.extract(gzfile, idxfile, startoffset, readlen)
	if(data == ""):
		print "No data"
		return results
	num_lines = 0
	num_results = 0
	got_result = False
	
	if is_regex:
		print "Compiling regex %s" % (searchterm)
		pat = re2.compile(searchterm)
	
	for line in data.splitlines():
		got_result = False
		if is_regex:
			if(re2.search(pat,line)):
				got_result = True
			else:
				got_result = False
		else:
			got_result = (searchterm in line)
		if (got_result == True):
			cur.execute("INSERT INTO result (jobcode, file_id, chunk_uoffset, linenum, line) VALUES (%s, %s, %s, %s, %s);", (jobcode, file_id, startoffset, (num_lines+1), (line.rstrip('\n'))))
			num_results += 1
		num_lines += 1
		
	conn.commit()
	conn.close()
	return ("Searched %d lines and found %d hits" % (num_lines,num_results))

def resolve_filename(filename):

	localfile = localdir + filename
	remotefile = remotedir + filename
	
	if(os.path.isfile(localfile)):
		return localfile
	
	if(os.path.isfile(remotefile)):
		return remotefile
	
	return ""
