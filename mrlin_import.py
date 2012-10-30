#!/usr/bin/env python
"""
mrlin - import

Imports an RDF/NTriples document concerning a graph URI into an HBase table. 
See https://github.com/mhausenblas/mrlin/wiki/RDF-in-HBase for details.

Usage: python mrlin_import.py path/to/file | URL
Examples: 
       python mrlin_import.py data/Galway.ntriples http://example.org/
       python mrlin_import.py http://dbpedia.org/data/Galway.ntriples http://example.org/

Copyright (c) 2012 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2012-10-27
@status: init
"""

import sys, os, logging, datetime, time, urllib, urllib2, json, requests, urlparse, ntriples, base64, happybase
from mrlin_utils import *

###############
# Configuration
DEBUG = True

if DEBUG:
	FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
	logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
	FORMAT = '%(asctime)-0s %(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')


#################################
# mrlin HBase interfacing classes

HBASE_BATCH_SIZE = 100

# patch the ntriples.Literal class
class SimpleLiteral(ntriples.Node):
	"""Represents a simple, stripped RDF literal object."""
	def __new__(cls, lit, lang=None, dtype=None):
		# Note that the parsed object value in the default implementation is 
		# encoded as follows: '@LANGUAGE ^^DATATYPE" VALUE'
		# For example:
		#  http://dbpedia.org/resource/Hildegarde_Naughton ... URI
		#  fi None Galway ... 'Galway'@fi
		#  None http://www.w3.org/2001/XMLSchema#int 14 ... '14'^^<http://www.w3.org/2001/XMLSchema#int>
		# n = str(lang) + ' ' + str(dtype) + ' ' + lit
		return unicode.__new__(cls, lit)
	
ntriples.Literal = SimpleLiteral
# END OF patch the ntriples.Literal class

class HBaseSink(ntriples.Sink): 
	"""Represents a sink for HBase."""
	def __init__(self, server_port, graph_uri): 
		"""Inits the HBase sink. The server_port must be set to the port the Thrift server is listening.
		   See http://wiki.apache.org/hadoop/Hbase/ThriftApi for details.
		"""
		self.length = 0
		self.server_port = server_port # Thrift server port
		self.graph_uri = graph_uri # the target graph URI for the document
		self.property_counter = {}
		self.starttime = time.time()
		self.time_delta = 0
		# prepare the RDF table in HBase using Thrift interface:
		self.hbm = HBaseThriftManager(host='localhost', server_port=self.server_port)
		self.hbm.init()
		self.batch = self.hbm.connection.table('rdf').batch()
	
	def triple(self, s, p, o): 
		"""Processes one triple as arriving in the sink."""
		if self.length == 0 : # we're starting the import task, source is ready
			self.time_delta = time.time() - self.starttime
			self.starttime = time.time()
			logging.info('== STATUS ==')
			logging.info(' Time to retrieve source: %.2f sec' %(self.time_delta))
		
		self.length += 1
		
		if DEBUG: logging.debug('Adding triple #%s: %s %s %s' %(self.length, s, p, o))
		
		if self.length % HBASE_BATCH_SIZE == 0: # we have $batch_size triples processed, send batch and show stats
			self.batch.send()
			self.batch = self.hbm.connection.table('rdf').batch()
			self.time_delta = time.time() - self.starttime
			self.starttime = time.time()
			logging.info('== STATUS ==')
			logging.info(' Time elapsed since last checkpoint:  %.2f sec' %(self.time_delta))
			logging.info(' Import speed: %.2f triples per sec' %(HBASE_BATCH_SIZE/self.time_delta))
		 
		self.add_row_thrift(g=self.graph_uri,s=s,p=p,o=o)
	
	def wrapup(self):
		self.batch.send()
		self.time_delta = time.time() - self.starttime
		self.starttime = time.time()
		logging.info('== FINAL STATUS ==')
		logging.info(' Time elapsed since last checkpoint:  %.2f sec' %(self.time_delta))
		logging.info(' Import speed: %.2f triples per sec' %(HBASE_BATCH_SIZE/self.time_delta))
		
	def add_row_thrift(self, g, s, p, o):
		"""Inserts an RDF triple as a row with subject as key using the Thrift interface via Happybase."""
		# make sure to store each property-object pair in its own column -
		# for details see https://github.com/mhausenblas/mrlin/wiki/RDF-in-HBase
		if s in self.property_counter: 
			self.property_counter[s] += 1
		else:
			self.property_counter[s] = 1
			
		self.batch.put(s, {	'G:': g,
						'P:' + str(self.property_counter[s]) : p,
						'O:' + str(self.property_counter[s]) : repr(o) })
	

#######################
# CLI auxilary methods

def import_ntriples(source, graph_uri='http://example.org'):
	"""Imports RDF/NTriples from directory, single file or URL."""
	starttime = time.time()
	imported_triples = 0
	
	if os.path.isdir(source): # we have a directory in the local file system with RDF/NTriples files
		logging.info('Importing RDF/NTriples from directory %s into graph %s' %(os.path.abspath(source), graph_uri))
		logging.info('='*12)
		imported_triples = _import_directory(source, graph_uri=graph_uri)
	elif source[:5] == 'http:': # we have a URL where we get the RDF/NTriples file from
		logging.info('Importing RDF/NTriples from URL %s into graph %s' %(source, graph_uri))
		logging.info('='*12)
		imported_triples = _import_data(src = urllib.urlopen(source), graph_uri=graph_uri)
	else: # we have a single RDF/NTriples file from the local file system
		logging.info('Importing RDF/NTriples from file %s into graph %s' %(source, graph_uri))
		logging.info('='*12)
		imported_triples = _import_data(src = open(source), graph_uri=graph_uri)
		
	deltatime = time.time() - starttime
	logging.info('='*12)
	logging.info('Imported %d triples in %.2f seconds.' %(imported_triples, deltatime))
	

def _import_directory(src_dir, graph_uri):
	"""Imports RDF/NTriples from directory."""
	imported_triples = 0
	for dirname, dirnames, filenames in os.walk(src_dir):
		for filename in filenames:
			if filename.endswith(('nt','ntriples')):
				logging.info('Importing RDF/NTriples from file %s into graph %s' %(filename, graph_uri))
				imported_triples += _import_data(src = urllib.urlopen(os.path.join(src_dir, filename)), graph_uri=graph_uri)
	return imported_triples

def _import_data(src, graph_uri):
	"""Imports RDF/NTriples from a single source, either local file or via URL."""
	nt_parser = ntriples.NTriplesParser(sink=HBaseSink(server_port=HBASE_THRIFT_PORT, graph_uri=graph_uri))
	sink = nt_parser.parse(src)
	sink.wrapup() # needed as the number of triples can be smaller than batch size (!)
	src.close()
	return sink.length


#############
# Main script

if __name__ == '__main__':
	try:
		if len(sys.argv) == 3: 
			inp = sys.argv[1]
			graph_uri = sys.argv[2]
			import_ntriples(inp, graph_uri)
		else: print __doc__
	except Exception, e:
		logging.error(e)
		sys.exit(2)
