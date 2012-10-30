# mrlin - MapReduce processing of Linked Data

> ...because it's magic

The basic idea of **mrlin** is to enable **M**ap **R**educe processing of **Lin**ked Data (hence the name). In the following I'm going to show you first to how to use HBase to store Linked Data with RDF and then how to use Hadoop to do execute MapReduce jobs.

## Background

### Dependencies

* You'll need [Apache HBase](http://hbase.apache.org/) first. I downloaded [`hbase-0.94.2.tar.gz`](http://ftp.heanet.ie/mirrors/www.apache.org/dist/hbase/stable/hbase-0.94.2.tar.gz) and then followed the [quickstart](http://hbase.apache.org/book/quickstart.html) up to section 1.2.3. to set it up.
* The Python scripts depend on [Happybase](https://github.com/wbolster/happybase) - see also the [docs](http://happybase.readthedocs.org/en/latest/index.html)

### Representing RDF triples in HBase
Learn about how mrlin represents [RDF triples in HBase](https://github.com/mhausenblas/mrlin/wiki/RDF-in-HBase).

### RESTful Interaction with HBase
Dig into [RESTful interactions](https://github.com/mhausenblas/mrlin/wiki/RESTful-interaction) with HBase, in mrlin.

## Usage

I assume you have HBase installed in some directory `HBASE_HOME` and mrlin in some other directory `MRLIN_HOME`.

First let's make sure that Happybase is installed correctly - we will use a [virtualenv](http://pypi.python.org/pypi/virtualenv "virtualenv 1.8.2 : Python Package Index"). You only need to do this once: go to `MRLIN_HOME` and type:

	$ virtualenv hb

Time to launch HBase and the Thrift server: in the `HBASE_HOME` directory, type the following:

	$ ./bin/start-hbase.sh 
	$ ./bin/hbase thrift start -p 9191

OK, now we're ready to launch mrlin - change to the directory `MRLIN_HOME` and first activate the virtualenv we created earlier:

	$ source hb/bin/activate

You should see a change in the prompt to something like `(hb)michau@~/Documents/dev/mrlin$` ... and now try to import a simple RDF NTriples file:

	$ (hb)michau@~/Documents/dev/mrlin$ python import_ntriples.py data/test_0.ntriples http://example.org/

If this works, try to import something bigger, for example:

	(hb)michau@~/Documents/dev/mrlin$ python import_ntriples.py http://dbpedia.org/data/Galway.ntriples http://dbpedia.org/
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

## License

All artifacts in this repository are licensed under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Software License.