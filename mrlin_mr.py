#!/usr/bin/env python
"""
mrlin - map/reduce

Allows to run Hadoop (MapReduce) jobs on mrlin tables.

Usage: python mrlin_mr.py param
Examples: 
       python mrlin_mr.py abc

Copyright (c) 2012 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2012-10-31
@status: init
"""
from mrjob.job import MRJob

class MREntityTypeCounter(MRJob):
	"""Calculates the types of entities (rdf:type) in a mrlin table."""
	# def __init__(self, *args, **kwargs):
	# 	super(MRInitJob, self).__init__(*args, **kwargs)
	
	def get_etypes(self, key, line):
		for word in line.split():
			yield word, 1
	
	def sum_etypes(self, word, occurrences):
		yield word, sum(occurrences)
	
	def steps(self):
		return [self.mr(self.get_etypes, self.sum_etypes),]
	

#############
# Main script

if __name__ == '__main__':
	MREntityTypeCounter.run()