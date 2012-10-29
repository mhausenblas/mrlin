#---
# Imports an RDF/NTriples document concerning a graph URI into an HBase table
# with the following schema: create 'rdf', 'G', 'P', 'O' ... note that the script
# assumes that the table exists already.
#---
import 'org.apache.hadoop.hbase.client.HTable'
import 'org.apache.hadoop.hbase.client.Put'

def tob(*args)
  args.map { |arg| arg.to_s.to_java_bytes }
end

def add_triple(table, g, s, p, o)
  p = Put.new(*tob(s))
  p.add( *tob("G", "", g))
  p.add( *tob("P", 1, p))
  p.add( *tob("O", 1, o))
  table.put(p)
end
  
table = HTable.new(@hbase.configuration, "rdf")
add_triple(table, "http://abc.com", "http://abc.com/#home", "http://www.w3.org/2000/01/rdf-schema#label", "ABC home")