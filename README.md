# mrlin - MapReduce processing of Linked Data

> ...because it's magic

The basic idea of **mrlin** is to enable **M**ap **R**educe processing of **Lin**ked Data - hence the name. In the following I'm going to show you first to how to use HBase to store Linked Data with RDF, and then how to use Hadoop to run MapReduce jobs.

## Background

### Dependencies

* You'll need [Apache HBase](http://hbase.apache.org/) first. I downloaded [`hbase-0.94.2.tar.gz`](http://ftp.heanet.ie/mirrors/www.apache.org/dist/hbase/stable/hbase-0.94.2.tar.gz) and followed the [quickstart](http://hbase.apache.org/book/quickstart.html) up to section 1.2.3. to set it up.
* The mrlin Python scripts depend on:
 * [Happybase](https://github.com/wbolster/happybase) to manage HBase; see also the [docs](http://happybase.readthedocs.org/en/latest/index.html) for further details.
 * [mrjob](https://github.com/Yelp/mrjob) to run MapReduce jobs; see also the [docs](http://packages.python.org/mrjob/) for further details.

### Representing RDF triples in HBase
Learn about how mrlin represents [RDF triples in HBase](https://github.com/mhausenblas/mrlin/wiki/RDF-in-HBase).

### RESTful Interaction with HBase
Dig into [RESTful interactions](https://github.com/mhausenblas/mrlin/wiki/RESTful-interaction) with HBase, in mrlin.

## Usage

### Setup
I assume you have HBase installed in some directory `HBASE_HOME` and mrlin in some other directory `MRLIN_HOME`. First let's make sure that Happybase is installed correctly - we will use a [virtualenv](http://pypi.python.org/pypi/virtualenv "virtualenv 1.8.2 : Python Package Index"). You only need to do this once: go to `MRLIN_HOME` and type:

	$ virtualenv hb

Time to launch HBase and the Thrift server: in the `HBASE_HOME` directory, type the following:

	$ ./bin/start-hbase.sh 
	$ ./bin/hbase thrift start -p 9191

OK, now we're ready to launch mrlin - change to the directory `MRLIN_HOME` and first activate the virtualenv we created earlier:

	$ source hb/bin/activate

You should see a change in the prompt to something like `(hb)michau@~/Documents/dev/mrlin$` ... and this means we're good to go!

### Import RDF/NTriples
To import  [RDF NTriples](http://www.w3.org/TR/rdf-testcases/#ntriples) documents, use the [`mrlin import`](https://raw.github.com/mhausenblas/mrlin/master/mrlin_import.py) script.

First, try to import **a file** from the local filesystem. Note the second parameter (`http://example.org/`), which specifies the target graph URI to import into:

	$ (hb)michau@~/Documents/dev/mrlin$ python mrlin_import.py data/test_0.ntriples http://example.org/

If this works, try to import directly from **a URL** `http://dbpedia.org/data/Galway.ntriples`:

	(hb)michau@~/Documents/dev/mrlin$ python mrlin_import.py http://dbpedia.org/data/Galway.ntriples http://dbpedia.org/
	2012-10-30T08:56:21 Initialized mrlin table.
	2012-10-30T08:56:31 Importing RDF/NTriples from URL http://dbpedia.org/data/Galway.ntriples into graph http://dbpedia.org/
	2012-10-30T08:56:31 == STATUS ==
	2012-10-30T08:56:31  Time to retrieve source: 9.83 sec
	2012-10-30T08:56:31 == STATUS ==
	2012-10-30T08:56:31  Time elapsed since last checkpoint:  0.07 sec
	2012-10-30T08:56:31  Import speed: 1506.61 triples per sec
	2012-10-30T08:56:31 == STATUS ==
	2012-10-30T08:56:31  Time elapsed since last checkpoint:  0.02 sec
	2012-10-30T08:56:31  Import speed: 4059.10 triples per sec
	2012-10-30T08:56:31 ==========
	2012-10-30T08:56:31 Imported 233 triples.

Note that you can also import **an entire directory** (mrlin will look for `.nt` and `.ntriples` files):
	
	(hb)michau@~/Documents/dev/mrlin$ python mrlin_import.py data/ http://example.org/
	2012-10-30T03:55:18 Importing RDF/NTriples from directory /Users/michau/Documents/dev/mrlin/data into graph http://example.org/
	...
	
To reset the HBase table (and remove all triples from it), use the [`mrlin utils`](https://raw.github.com/mhausenblas/mrlin/master/mrlin_utils.py) script like so:

	(hb)michau@~/Documents/dev/mrlin$ python mrlin_utils.py clear

### Query
In order to query the mrlin datastore in HBase, use the [`mrlin query`](https://raw.github.com/mhausenblas/mrlin/master/mrlin_query.py) script:

	(hb)michau@~/Documents/dev/mrlin$ python mrlin_query.py Tribes
	2012-10-30T04:01:22 Scanning table rdf with filter ValueFilter(=,'substring:Tribes')
	2012-10-30T04:01:22 Key: http://dbpedia.org/resource/Galway - Value: {'O:148': 'u\'"City of the Tribes"\'', 'O:66': 'u\'"City of the Tribes"\'',  ...}
	2012-10-30T04:01:22 ============
	2012-10-30T04:01:22 Query took me 0.01 seconds.

### Running MapReduce jobs

*TBD*

* setup in virtual env: `source hb/bin/activate` then `pip install mrjob`
* `cp .mrjob.conf ~` before launch
* `source hb/bin/activate`
* run `python mrlin_mr.py README.md` for standalone
* set up [Hadoop 1.0.4](http://ftp.heanet.ie/mirrors/www.apache.org/dist/hadoop/common/hadoop-1.0.4/hadoop-1.0.4.tar.gz) - if unsure follow a [single-node setup](http://orzota.com/blog/single-node-hadoop-setup-2/)  tutorial
* `cp .mrjob.conf ~` before launch if you change settings (!)
* note all changes that were necessary in ` conf/core-site.xml`, `conf/mapred-site.xml`, `conf/hdfs-site.xml`, and `hadoop-env.sh` (provide examples)
* run `python mrlin_mr.py -r hadoop README.md` for local Hadoop 


#### Debug

* `tail -f hadoop-michau-namenode-Michael-Hausenblas-iMac.local.log`



## License

All artifacts in this repository are licensed under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Software License.