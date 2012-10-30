#!/usr/bin/env python
"""
mrlin - query

Provides query and look-up facilities for mrlin tables. 
See https://github.com/mhausenblas/mrlin/wiki/RDF-in-HBase for details.

Usage: python mrlin_query.py pattern
Examples: 
       python mrlin_query.py Galway

Copyright (c) 2012 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2012-10-30
@status: init
"""

import sys, os, logging, datetime, time, urllib, urllib2, json, requests, urlparse, ntriples, base64, happybase
from mrlin_utils import *

###############
# Configuration
DEBUG = False

if DEBUG:
	FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
	logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
	FORMAT = '%(asctime)-0s %(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')


#######################
# CLI auxilary methods

def query(pattern):
	"""Query via scan."""
	starttime = time.time()
	
	hbm = HBaseThriftManager(host='localhost', server_port=HBASE_THRIFT_PORT)
	hbm.scan_table(HBASE_TABLE_RDF, pattern)
	
	deltatime = time.time() - starttime
	logging.info('='*12)
	logging.info('Query took me %.2f seconds.' %(deltatime))
	


#############
# Main script

if __name__ == '__main__':
	try:
		if len(sys.argv) == 2: 
			pattern = sys.argv[1]
			query(pattern)
		else: print __doc__
	except Exception, e:
		logging.error(e)
		sys.exit(2)
