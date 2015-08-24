package de.unihamburg.vsis.sddf.indexing.blocking

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.reading.Tuple

trait BlockingPipe extends PipeElement[RDD[Tuple], RDD[Seq[Tuple]]]