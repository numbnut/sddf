package de.unihamburg.vsis.sddf.clustering

import scala.Iterator

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD

import de.unihamburg.vsis.sddf.reading.Tuple

/**
 * This clustering algorithm keeps the strongest paths.
 * Every node selects one edge with the highest value.
 * If there are more than one with the highest value, all with the highest value get selected.
 * All not selected edges get removed.
 * Every connected component represents one cluster.
 */
class PipeClusteringStrongestPath extends PipeClusteringTransitiveClosure {
  
  override def manipulateGraph(graph: Graph[Tuple, Double]): Graph[_, Double] = {

    val cGraph = graph.mapVertices((vid, tuple) => (vid, Double.MinPositiveValue))

    // attach the max adjacent edge attribute to each vertice
    val verticesMaxEdgeAttributes: VertexRDD[Double] = cGraph.mapReduceTriplets(
      edge => {
        Iterator((edge.dstId, edge.attr), (edge.srcId, edge.attr))
      },
      (a: Double, b: Double) => math.max(a, b)
    )

    // join the resulting vertice attributes with the graph
    val maxGraph: Graph[(Tuple, Double), Double] =
      graph.outerJoinVertices(verticesMaxEdgeAttributes)((id, tuple, simOpt) =>
        simOpt match {
          case Some(sim) => (tuple, sim)
          case None      => (tuple, 0D)
        }
      )
      
    // remove edges which have a max value less then src or dst 
    val resultGraph = maxGraph.subgraph(edge => {
      if (edge.attr < edge.srcAttr._2 && edge.attr < edge.dstAttr._2) {
        false
      } else {
        true
      }
    })
    resultGraph
  }

}

object PipeClusteringStrongestPath {
  
  def apply() = new PipeClusteringStrongestPath()

}