
* We don't need separate node, way and relation maps, recombine them
  and use 2 bits of the key for marking the type of node

* Get rid of the Java (Tree)Set implementation in HeapMap as it is 
  memory exhaustion. Write a memory optimized version.
  
* at least some tests

* expand maps when full

