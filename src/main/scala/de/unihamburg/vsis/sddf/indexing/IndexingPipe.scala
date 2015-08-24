package de.unihamburg.vsis.sddf.indexing

import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable
import de.unihamburg.vsis.sddf.Parameterized

trait IndexingPipe extends PipeElement[RDD[Tuple], RDD[SymPair[Tuple]]]