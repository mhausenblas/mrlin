# mrlin - MapReduce

> ...because it's magic

The basic idea of **mrlin** is to enable **M**ap**R**educe processing of **Lin**ked Data (hence the name). In the following I'm going to show you first to how to use HBase to store Linked Data with RDF and then how to use Hadoop to do execute MapReduce jobs.

## Dependencies

* You'll need [Apache HBase](http://hbase.apache.org/) first. I downloaded [`hbase-0.94.2.tar.gz`](http://ftp.heanet.ie/mirrors/www.apache.org/dist/hbase/stable/hbase-0.94.2.tar.gz) and then followed the [quickstart](http://hbase.apache.org/book/quickstart.html) up to section 1.2.3.

## Representing RDF triples in an HBase table

The mapping of an RDF graph with `TRIPLE_i = <graph_i, subject_i, predicate_i, object_i>` to a HBase table called `rdf` is as follows:

* The row key is the subject `subject_i` of `TRIPLE_i` in the current entity.
* There is a column family called graph, or `G` for short and the value being `graph_i` of `TRIPLE_i`.
* There is a second column family called predicate, or `P` for short, with the key being the count of the predicate in the current entity and the value being `predicate_i` of `TRIPLE_i`.
* There is a third column family called object, or `O` for short, with the key being the count of the corresponding predicate `predicate_i` and  the value being `object_i` of `TRIPLE_i`.

So, for example the RDF entity in the graph `http://example.org` ...

	<http://mhausenblas.info/#i> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .
	<http://mhausenblas.info/#i> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .

... would be mapped to the HBase table `rdf` as follows:

	[row keys]                    [column family 'G']       [column family 'P']                                   [column family 'O'] 
	
	'http://mhausenblas.info/#i'  '':'http://example.org'  '1':'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'  '1':'http://schema.org/Person'
	                                                       '2':'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'  '2':'http://xmlns.com/foaf/0.1/Person'

We will manage the HBase table called `rdf` via the shell, so go to the directory where you've installed HBase and launch it:

	$ ./bin/hbase shell

### Creating the HBase table
First, you want to define the scheme of the table in the shell like so:

	hbase(main):001:0> create 'rdf', 'G', 'P', 'O' 
	0 row(s) in 1.0340 seconds

### Populating the HBase table
Cool, that worked nicely. Let's put some data into our newly created table, shall we?

	hbase(main):002:0> put 'rdf', 'http://mhausenblas.info/#i', 'G:', 'http://example.org'
	hbase(main):003:0> put 'rdf', 'http://mhausenblas.info/#i', 'P:1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
	hbase(main):004:0> put 'rdf', 'http://mhausenblas.info/#i', 'O:1', 'http://schema.org/Person'

Now that was easy, wasn't it? But - is the data really stored correctly? Let's check that:

	hbase(main):005:0> scan 'rdf'
	ROW                           COLUMN+CELL                                                                                                                                                                                                              
	 http://mhausenblas.info/#i   column=G:, timestamp=1351372113753, value=http://example.org                                                                                                                                                             
	 http://mhausenblas.info/#i   column=O:1, timestamp=1351372247689, value=http://schema.org/Person                                                                                                                                                      
	 http://mhausenblas.info/#i   column=P:1, timestamp=1351372243242, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#type
	1 row(s) in 0.0420 seconds

Right, looks good. How about adding another `rdf:type` information, such as `foaf:Person`?

	hbase(main):006:0> put 'rdf', 'http://mhausenblas.info/#i', 'P:2', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
	hbase(main):007:0> put 'rdf', 'http://mhausenblas.info/#i', 'O:2', 'http://xmlns.com/foaf/0.1/Person'
	
... and again let's see how we're doing:

	hbase(main):008:0> scan 'rdf'

	ROW                          COLUMN+CELL                                                                                                                                                                                                              
	 http://mhausenblas.info/#i  column=G:, timestamp=1351372113753, value=http://example.org                                                                                                                                                             
	 http://mhausenblas.info/#i  column=O:1, timestamp=1351372247689, value=http://schema.org/Person                                                                                                                                                      
	 http://mhausenblas.info/#i  column=O:2, timestamp=1351372555296, value=http://xmlns.com/foaf/0.1/Person                                                                                                                                              
	 http://mhausenblas.info/#i  column=P:1, timestamp=1351372243242, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#type                                                                                                                               
	 http://mhausenblas.info/#i  column=P:2, timestamp=1351372547894, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#type                                                                                                                               
	1 row(s) in 0.0590 seconds

And now we add another predicate-object pair to the entity identified by `http://mhausenblas.info/#i`:

	hbase(main):009:0> put 'rdf', 'http://mhausenblas.info/#i', 'P:3', 'http://www.w3.org/2000/01/rdf-schema#label'
	hbase(main):010:0> put 'rdf', 'http://mhausenblas.info/#i', 'O:3', 'Michael'

### Querying the HBase table

OK, that was already rather entertaining so far. How about some querying now? Let's list everything HBase knows about the entity `http://mhausenblas.info/#i`:

	hbase(main):011:0> get 'rdf', 'http://mhausenblas.info/#i'
	COLUMN  CELL                                                                                                                                                                                                                     
	 G:      timestamp=1351372113753, value=http://example.org                                                                                                                                                                        
	 O:1     timestamp=1351372247689, value=http://schema.org/Person                                                                                                                                                                  
	 O:2     timestamp=1351372555296, value=http://xmlns.com/foaf/0.1/Person                                                                                                                                                          
	 O:3     timestamp=1351373497310, value=Michael                                                                                                                                                                                   
	 P:1     timestamp=1351372243242, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#type                                                                                                                                           
	 P:2     timestamp=1351372547894, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#type                                                                                                                                           
	 P:3     timestamp=1351373490159, value=http://www.w3.org/2000/01/rdf-schema#label                                                                                                                                                
	7 row(s) in 0.0350 seconds

Or how about only the first predicate-object pair? Try:

	hbase(main):012:0> get 'rdf', 'http://mhausenblas.info/#i', 'P:1', 'O:1'
	COLUMN  CELL                                                                                                                                                                                                                     
	 O:1     timestamp=1351372247689, value=http://schema.org/Person                                                                                                                                                                  
	 P:1     timestamp=1351372243242, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#type                                                                                                                                           
	2 row(s) in 0.0260 seconds


Scanning for entities that contain a certain string in the object value (we add another entity first to demonstrate this better):
	
	hbase(main):013:0> put 'rdf', 'http://example.org/#dummy', 'G:', 'http://example.org'
	hbase(main):014:0> put 'rdf', 'http://example.org/#dummy', 'P:1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
	hbase(main):015:0> put 'rdf', 'http://example.org/#dummy', 'O:1', 'http://schema.org/Thing'

Then:
	hbase(main):016:0> scan 'rdf', { COLUMNS => ['O'], FILTER => "ValueFilter(=,'substring:Michael')"}
	ROW                          COLUMN+CELL                                                                                                                                                                                                              
	 http://mhausenblas.info/#i  column=O:3, timestamp=1351373497310, value=Michael                                                                                                                                                                       
	1 row(s) in 0.0110 seconds

And now a bit more advanced, scanning for entities that contain `schema.org` somewhere:

	hbase(main):017:0> scan  'rdf', { COLUMNS => ['P', 'O'], FILTER => "ValueFilter(=,'regexstring:.*schema.org')"}
	ROW                          COLUMN+CELL                                                                                                                                                                                                              
	 http://example.org/#dummy   column=O:1, timestamp=1351377136816, value=http://schema.org/Thing                                                                                                                                                       
	 http://mhausenblas.info/#i  column=O:1, timestamp=1351372247689, value=http://schema.org/Person                                                                                                                                                      
	2 row(s) in 0.0330 seconds

### Cleaning up ...
We're done for now, so if you don't need the HBase table anymore it's time to get rid of it:

	> disable 'rdf'
	> drop 'rdf'
	> exit

... and once you're out of the HBase shell, don't forget to send HBase to bed:

	$ ./bin/stop-hbase.sh
	stopping hbase...............

## License

All artifacts in this repository are licensed under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Software License.