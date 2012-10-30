#!/usr/bin/env python
"""
mrlin - utils

Utilities for HBase interaction.

Usage: python mrlin_utils.py init | clear
Examples: 
       python import_ntriples.py init
       python import_ntriples.py clear

Copyright (c) 2012 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2012-10-30
@status: init
"""

import sys, logging, datetime, time, happybase
from os import curdir, sep

###############
# Configuration
DEBUG = False

if DEBUG:
	FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
	logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
	FORMAT = '%(asctime)-0s %(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')


#################################
# mrlin HBase interfacing classes

HBASE_TABLE_RDF = 'rdf'
HBASE_STARGATE_PORT = 9191
HBASE_THRIFT_PORT = 9191

class HBaseThriftManager(object):
	"""Represents a Thrift-based HBase manager using Happybase http://happybase.readthedocs.org/"""
	def __init__(self, host, server_port): 
		self.host = host
		self.server_port = server_port
		self.connection = happybase.Connection(host=self.host, port=self.server_port)
	
	def create_table(self, table_name, col_fam):
		"""Creates a table, if does not yet exist."""
		current_tables = self.connection.tables()
		if table_name not in current_tables:
			self.connection.create_table(table_name, col_fam)
			if DEBUG: logging.debug('Created table %s.' %(table_name))
		else:
			if DEBUG: logging.debug('Table %s already exists!' %(table_name))
	
	def init(self):
		"""Inits the mrlin table. See https://github.com/mhausenblas/mrlin/wiki/RDF-in-HBase for details."""
		self.create_table(table_name=HBASE_TABLE_RDF, col_fam={'G': {}, 'P': {}, 'O': {}} )
		logging.info('Initialized mrlin table.')
	
	def clear(self):
		"""Disables and drops the mrlin table."""
		current_tables = self.connection.tables()
		if HBASE_TABLE_RDF in current_tables:
			self.connection.disable_table(HBASE_TABLE_RDF)
			self.connection.delete_table(HBASE_TABLE_RDF)
			logging.info('Cleared mrlin table.')
		else:
			logging.info('The mrlin table did not exist, no action taken.')
	
	def scan_table(self, table_name, pattern=None):
		"""Scans a table using filter"""
		table = self.connection.table(table_name)
		if pattern:
			if all(ord(c) < 128 for c in pattern): # we have a pure ASCII string
				p = pattern
			else: # @@TODO: fix me!!!
				p = repr(pattern)
				p = p[1:-1]
				
			filter_str = 'ValueFilter(=,\'substring:%s\')' %str(p)
			logging.info('Scanning table %s with filter %s' %(table_name, str(filter_str)))
			for key, data in table.scan(filter=filter_str):
				logging.info('Key: %s - Value: %s' %(key, str(data)))
		else:
			logging.info('Scanning table %s' %(table_name))
			for key, data in table.scan():
				logging.info('Key: %s - Value: %s' %(key, data))
	

#############
# Main script

if __name__ == '__main__':
	try:
		if len(sys.argv) == 2:
			hbm = HBaseThriftManager(host='localhost', server_port=HBASE_THRIFT_PORT) 
			inp = sys.argv[1]
			if inp == 'init':
				hbm.init()
			elif inp == 'clear':
				hbm.clear()
			else:
				print __doc__
		else: print __doc__
	except Exception, e:
		logging.error(e)
		sys.exit(2)
