package de.unihamburg.vsis.sddf.clustering

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.similarity.aggregator.Mean
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

/**
 * Abstract class that builds a graph from tuple pairs that can be used to do clustering.
 * Every edge is associated with a Double value which represents the accumulated similarity.
 */
abstract class AbstractPipeClusteringGraph
  extends PipeElement[RDD[(SymPair[Tuple], Array[Double])], RDD[Set[Tuple]]]
  with Serializable {
  
  def cluster(graph: Graph[Tuple, Double]): RDD[Set[Tuple]]

  def step(input: RDD[(SymPair[Tuple], Array[Double])])(implicit pipeContext: AbstractPipeContext): RDD[Set[Tuple]] = {
    
    val duplicatePairsWithSimilarity = input.map(
      pair => (pair._1, Mean.agrSimilarity(pair._2))
    )
    
    val edges: RDD[Edge[Double]] = duplicatePairsWithSimilarity.map(
      pair => { Edge(pair._1._1.id, pair._1._2.id, pair._2) }
    )

    // TODO optimize: it would be nice to build the graph only by using edge triplets
    // but as far as I know that's not possible
    val verticesNotUnique: RDD[(VertexId, Tuple)] = duplicatePairsWithSimilarity.map(_._1).flatMap(
      tuplePair => Seq(tuplePair._1, tuplePair._2)
    ).map(tuple => (tuple.id, tuple))

    // delete all duplicate vertices
    val vertices = verticesNotUnique.distinct()

    // The edge type Boolean is just a workaround because no edge types are needed
    val graph: Graph[Tuple, Double] = Graph.apply(vertices, edges, null)
    
    cluster(graph)
  }

}
