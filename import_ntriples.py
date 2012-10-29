
"""
mrlin - tools - import

Imports an RDF/NTriples document concerning a graph URI into an HBase table
with the following schema: create 'rdf', 'G', 'P', 'O'.

Copyright (c) 2012 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2012-10-27
@status: init
"""

import sys, logging, datetime, urllib, urllib2, json, requests, urlparse
from os import curdir, sep

# configuration
DEBUG = True

if DEBUG:
	FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
	logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
	FORMAT = '%(asctime)-0s %(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')


def import_data():
	if DEBUG: logging.debug('GOT QUERY\n%s' %(query))
	query = urllib2.unquote(query)
	try:
		p = {"query": query } 
		headers = { 'Accept': 'application/sparql-results+json', 'Access-Control-Allow-Origin': '*' }
		logging.debug('Query to endpoint %s with query\n%s' %(endpoint, query))
		request = requests.get(endpoint, params=p, headers=headers)
		logging.debug('Request:\n%s' %(request.url))
		logging.debug('Result:\n%s' %(json.dumps(request.json, sort_keys=True, indent=4)))
		self.send_response(200)
		self.send_header('Content-type', 'application/json')
		self.end_headers()
		self.wfile.write(json.dumps(request.json))
	except:
		self.send_error(500, 'Something went wrong here on the server side.')

if __name__ == '__main__':
	try:
		import_data()
	except Exception, e:
		logging.error(e)
		sys.exit(2)