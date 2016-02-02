package com.svds.graphday

import org.apache.spark.graphx._

import scala.reflect.ClassTag


/**
  * Created by andrew on 12/8/15.
  */
object Examples {

  // Simplified version of org.apache.spark.graphx.lib.ConnectedComponents
  def connectedComponents[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexId, ED] = {
    graph
      .mapVertices { case (id, attr) => id }
      .pregel(initialMsg = Long.MaxValue)(
        vprog = (id, attr, msg) => math.min(attr, msg),
        sendMsg = edge => {
          if (edge.srcAttr < edge.dstAttr)
            Iterator((edge.dstId, edge.srcAttr))
          else if (edge.srcAttr > edge.dstAttr)
            Iterator((edge.srcId, edge.dstAttr))
          else
            Iterator.empty
        },
        mergeMsg = (a, b) => math.min(a, b)
      )
  }

  // Simplified version of org.apache.spark.graphx.lib.PageRank
  def pageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Graph[Double, Double] = {
    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (id, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets(e => 1.0 / e.srcAttr)
      .mapVertices((id, attr) => 1.0)
    for (i <- 1 to numIter) {
      val rankUpdates = rankGraph.aggregateMessages[Double](
        sendMsg = ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr),
        mergeMsg = (a, b) => a + b
      )
      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => 0.15 + 0.85 * msgSum
      }
    }
    rankGraph
  }

  // Based on example in https://spark.apache.org/docs/latest/graphx-programming-guide.html
  // see also org.apache.spark.graphx.lib.ShortestPaths
  def sssp[VD: ClassTag](graph: Graph[VD, Double], sourceId: VertexId): Graph[Double, Double] = {
    graph
      .mapVertices { (id, attr) =>
        if (id == sourceId) 0.0
        else Double.PositiveInfinity
      }.pregel(initialMsg = Double.PositiveInfinity)(
        vprog = (id, dist, msg) => math.min(dist, msg),
        sendMsg = edge => {
          if (edge.srcAttr + edge.attr < edge.dstAttr)
            Iterator((edge.dstId, edge.srcAttr + edge.attr))
          else
            Iterator.empty
        },
        mergeMsg = (a, b) => math.min(a, b)
      )
  }

  // Simplified version of org.apache.spark.graphx.lib.LabelPropagation
  def labelProp[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
    graph
      .mapVertices { case (id, attr) => id }
      .pregel(initialMsg = Map[VertexId, Long](), maxIterations = maxSteps)(
        vprog = (id, attr, msg) => if (msg.isEmpty) attr else msg.maxBy(_._2)._1,
        sendMsg = edge =>
          Iterator(
            (edge.srcId, Map(edge.dstAttr -> 1L)),
            (edge.dstId, Map(edge.srcAttr -> 1L))
          ),
        mergeMsg = (a, b) =>
          (a.keySet ++ b.keySet).map { k =>
            k -> (a.getOrElse(k, 0L) + b.getOrElse(k, 0L))
          }.toMap
      )
  }
}



