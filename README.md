# GraphX examples for Intro to Pregel and PowerGraph at Graph Day Texas 2016 

Build with sbt and include jar in spark-shell to try it out.

```
sbt package
spark-shell --jars target/scala-2.10/graph-day-texas-2016_2.10-1.0.jar
```

(or just copy and paste the [code](src/main/scala/com/svds/graphday/Examples.scala) into a spark-shell)

Then try it out

```scala
import org.apache.spark.graphx._
import com.svds.graphday.Examples

val edges = sc.parallelize(Array(Edge(1L, 2L, 1.0), Edge(2L, 3L, 1.0),
                                 Edge(1L, 3L, 3.0), Edge(3L, 4L, 2.0)))

val graph = Graph.fromEdges(edges, 0.0)

//All vertices are in one connected component
Examples.connectedComponents(graph).vertices.collect
//Array((1,1), (2,1), (3,1), (4,1))

//Page rank of the vertices
Examples.pageRank(graph, 10).vertices.collect
//Array((1,1.0), (2,0.575), (3,1.06375), (4,1.0541874999999998))

//Single source shortes path starting at vertex 1
Examples.sssp(graph, 1).vertices.collect
//Array((1,0.0), (2,1.0), (3,2.0), (4,4.0))

//Label propigation gives two "clusters"
Examples.labelProp(graph,10).vertices.collect
//Array((1,1), (2,2), (3,1), (4,2))
```

This code is not meant for production use. Each algorithm is a simplifications of the official implementations.
