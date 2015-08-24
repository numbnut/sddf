package de.unihamburg.vsis.sddf.clustering

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.reading.Tuple

/**
 * Computes the connected components and returns them.
 */
class PipeClusteringTransitiveClosure
  extends AbstractPipeClusteringGraph
  with Logging
  with Serializable {
  
  override def cluster(graph: Graph[Tuple, Double]): RDD[Set[Tuple]] = {
    // compute the connected components
    val cc: Graph[VertexId, Double] = manipulateGraph(graph).connectedComponents
    log.debug("Number of Connected Components :" + cc.numVertices)
    val ccVertices: VertexRDD[(VertexId, Set[Tuple])] = graph.vertices.leftJoin(cc.vertices)({
      case (id, tuple, compId) => (compId.get, Set(tuple))
    })
    val ccTupleSets: RDD[(VertexId, Set[Tuple])] = ccVertices.map(_._2).reduceByKey(
      (a, b) => a ++ b
    )
    ccTupleSets.map(_._2)
  }
  
  /**
   * Override this method in child classes to return a manipulated graph,
   * which will be used afterwards to compute the connected components.
   */
  def manipulateGraph(graph: Graph[Tuple, Double]): Graph[_, Double] = {
    graph
  }

}

object PipeClusteringTransitiveClosure {
  
  def apply() = new PipeClusteringTransitiveClosure()

}