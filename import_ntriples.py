#!/usr/bin/env python
"""
mrlin - tools - import

Imports an RDF/NTriples document concerning a graph URI
into an HBase table with the following schema: 
create 'rdf', 'G', 'P', 'O'.

Usage: python import_ntriples.py path/to/file or URL
Examples: 
       python import_ntriples.py data/Galway.ntriples http://example.org/
       python import_ntriples.py http://dbpedia.org/data/Galway.ntriples http://example.org/

Copyright (c) 2012 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2012-10-27
@status: init
"""

import sys, logging, datetime, urllib, urllib2, json, requests, urlparse, ntriples, base64
from os import curdir, sep

###############
# Configuration
DEBUG = True

if DEBUG:
	FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
	logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
	FORMAT = '%(asctime)-0s %(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')

HBASE_TABLE_RDF = 'rdf'
HBASE_STARGATE_PORT = 9191

#################################
# mrlin HBase interfacing classes

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
	def __init__(self, stargate_port, graph_uri): 
		"""Inits the HBase sink."""
		self.length = 0
		self.stargate_port = stargate_port
		self.graph_uri = graph_uri

	def triple(self, s, p, o): 
		"""Processes one triple as arriving in the sink."""
		self.length += 1
		if DEBUG: logging.debug('%s %s %s' %(s, p, o)) 

	def add_row(self, g, s, p, o):
		"""Inserts one RDF triple as a row with subject as key."""
		row_key = urllib2.quote(s)
		headers = { 'Content-Type:': 'text/xml' }
		url = 'http://localhost:' + self.stargate_port + '/' + HBASE_TABLE_RDF + '/' + row_key  + '/G'
		cell_payload = """
		<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
		<CellSet>
			<Row key="%s">
				<Cell column="G:">%s</Cell>
			</Row>
		</CellSet>
		""" %(base64.b64encode(s), base64.b64encode(self.graph_uri))
		r = requests.post(url, data=cell_payload, headers=headers)
		if DEBUG: logging.debug('%s' %(r.text)) 


#######################
# CLI auxilary methods

def import_data_file(input_file, graph_uri):
	if DEBUG: logging.debug('Importing RDF/NTriples from file %s into HBase' %(input_file))
	nt_parser = ntriples.NTriplesParser(sink=HBaseSink(HBASE_STARGATE_PORT, graph_uri))
	f = open(input_file)
	sink = nt_parser.parse(f)
	f.close()
	logging.info('Imported %d triples.' %(sink.length))
	
def import_data_URL(input_url, graph_uri): 
	if DEBUG: logging.debug('Importing RDF/NTriples from URL %s into HBase' %(input_url))
	nt_parser = ntriples.NTriplesParser(sink=HBaseSink(HBASE_STARGATE_PORT, graph_uri))
	u = urllib.urlopen(input_url)
	sink = nt_parser.parse(u)
	u.close()
	logging.info('Imported %d triples.' %(sink.length))


#############
# Main script

if __name__ == '__main__':
	try:
		if len(sys.argv) == 3: 
			inp = sys.argv[1]
			graph_uri = sys.argv[2]
			if inp[:5] == 'http:':
				import_data_URL(inp, graph_uri)
			else:
				import_data_file(inp, graph_uri)
		else: print __doc__
	except Exception, e:
		logging.error(e)
		sys.exit(2)