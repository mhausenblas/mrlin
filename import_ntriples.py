#!/usr/bin/env python
"""
mrlin - tools - import

Imports an RDF/NTriples document concerning a graph URI
into an HBase table with the following schema: 
create 'rdf', 'G', 'P', 'O'.

Usage: python import_ntriples.py path/to/file or URL
Examples: 
       python import_ntriples.py data/Galway.ntriples
       python import_ntriples.py http://dbpedia.org/data/Galway.ntriples

Copyright (c) 2012 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2012-10-27
@status: init
"""

import sys, logging, datetime, urllib, urllib2, json, requests, urlparse, ntriples, pprint
from os import curdir, sep

# configuration
DEBUG = True

if DEBUG:
	FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
	logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
	FORMAT = '%(asctime)-0s %(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')

# patch the ntriples.Literal class
class SimpleLiteral(ntriples.Node):
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
	def __init__(self): 
		self.length = 0

	def triple(self, s, p, o): 
		self.length += 1
		if DEBUG: logging.debug('%s %s %s' %(s, p, o)) 

def import_data_file(input_file):
	if DEBUG: logging.debug('Importing RDF/NTriples from file %s into HBase' %(input_file))
	nt_parser = ntriples.NTriplesParser(sink=HBaseSink())
	f = open(input_file)
	sink = nt_parser.parse(f)
	f.close()
	logging.info('Imported %d triples.' %(sink.length))
	
def import_data_URL(input_url): 
	if DEBUG: logging.debug('Importing RDF/NTriples from URL %s into HBase' %(input_url))
	nt_parser = ntriples.NTriplesParser(sink=HBaseSink())
	u = urllib.urlopen(input_url)
	sink = nt_parser.parse(u)
	u.close()
	logging.info('Imported %d triples.' %(sink.length))

if __name__ == '__main__':
	try:
		if len(sys.argv) == 2: 
			inp = sys.argv[1]
			if inp[:5] == 'http:':
				import_data_URL(inp)
			else:
				import_data_file(inp)
		else: print __doc__
	except Exception, e:
		logging.error(e)
		sys.exit(2)