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

import sys, logging, datetime, urllib, urllib2, json, requests, urlparse, ntriples, base64, happybase
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
HBASE_THRIFT_PORT = 9191

#################################
# mrlin HBase interfacing classes

HBASE_METHOD_REST = 'REST'
HBASE_METHOD_THRIFT = 'THRIFT'

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
	def __init__(self, method, server_port, graph_uri): 
		"""Inits the HBase sink. The method must either be HBASE_METHOD_REST or HBASE_METHOD_THRIFT 
		   and the server_port must be set to the port Stargate or the Thrift server is listening."""
		self.length = 0
		self.method = method
		self.server_port = server_port
		self.graph_uri = graph_uri
		self.property_counter = {}
	
	def triple(self, s, p, o): 
		"""Processes one triple as arriving in the sink."""
		self.length += 1
		if DEBUG: logging.debug('Adding %s %s %s' %(s, p, o)) 
		if self.method == HBASE_METHOD_REST:
			self.add_row_rest(g=self.graph_uri,s=s,p=p,o=o)
		elif self.method == HBASE_METHOD_THRIFT:
			self.add_row_thrift(g=self.graph_uri,s=s,p=p,o=o)
	
	def add_row_thrift(self, g, s, p, o):
		"""Inserts an RDF triple as a row with subject as key using the Thrift interface via Happybase."""
		connection = happybase.Connection(host='localhost', port=self.server_port)
		table = connection.table('rdf')
		
		# make sure to store each property-object pair in its own column -
		# for details see https://github.com/mhausenblas/mrlin/wiki/RDF-in-HBase
		if s in self.property_counter: 
			self.property_counter[s] += 1
		else:
			self.property_counter[s] = 1
			
		table.put(s, {	'G:': g,
						'P:' + str(self.property_counter[s]) : p,
						'O:' + str(self.property_counter[s]) : o })
		
		# row = table.row('row-key')
		# print row['family:qual1']  # prints 'value1'
		# 
		# for key, data in table.rows(['row-key-1', 'row-key-2']):
		#     print key, data  # prints row key and data for each row
		# 
		# for key, data in table.scan(row_prefix='row'):
		#     print key, data  # prints 'value1' and 'value2'
		# 
		# row = table.delete('row-key')

	def add_row_rest(self, g, s, p, o):
		"""Inserts an RDF triple as a row with subject as key using the REST interface via Stargate."""
		row_key = urllib2.quote(s)
		headers = { 'Content-Type:': 'text/xml' }
		url = 'http://localhost:' + str(self.server_port) + '/' + HBASE_TABLE_RDF + '/' + row_key  + '/G'
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

def import_data(ntriples_doc, graph_uri):
	nt_parser = ntriples.NTriplesParser(sink=HBaseSink(method=HBASE_METHOD_THRIFT, server_port=HBASE_THRIFT_PORT, graph_uri=graph_uri))
	
	# sniffing input provided - this is really a very naive way of doing this
	if ntriples_doc[:5] == 'http:':
		src = urllib.urlopen(ntriples_doc)
		logging.info('Importing RDF/NTriples from URL %s into graph %s' %(ntriples_doc, graph_uri))
	else:
		src = open(ntriples_doc)
		logging.info('Importing RDF/NTriples from file %s into graph %s' %(ntriples_doc, graph_uri))
		
	sink = nt_parser.parse(src)
	src.close()
	logging.info('Imported %d triples.' %(sink.length))


#############
# Main script

if __name__ == '__main__':
	try:
		if len(sys.argv) == 3: 
			inp = sys.argv[1]
			graph_uri = sys.argv[2]
			import_data(inp, graph_uri)
		else: print __doc__
	except Exception, e:
		logging.error(e)
		sys.exit(2)
