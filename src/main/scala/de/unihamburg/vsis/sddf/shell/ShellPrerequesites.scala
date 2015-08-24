package de.unihamburg.vsis.sddf.shell;
// This file needs to be loaded by the spark-shell to avoid annoying imports
// ./spark-shell 
// --jars /home/niklas/projects/sddf/code/projects/simpleapp/target/scala-2.10/sddf-assembly-1.0.jar
// -i /home/niklas/projects/sddf/code/projects/simpleapp/src/main/scala/de/unihamburg/vsis/sddf/shell/ShellPrerequesits.scala

// import all measures
import com.rockymadden.stringmetric.similarity._

import de.unihamburg.vsis.sddf.similarity.measures.MeasureEquality
import de.unihamburg.vsis.sddf.similarity.measures.MeasureWrapperToLower

// context
import de.unihamburg.vsis.sddf.SddfContext._
import de.unihamburg.vsis.sddf.pipe.context.SddfPipeContext
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeStoreInContextGoldstandard
import de.unihamburg.vsis.sddf.reading.corpus.PipeStoreInContextCorpus

// reading
import de.unihamburg.vsis.sddf.reading.corpus.PipeStoreInContextCorpus
import de.unihamburg.vsis.sddf.reading.corpus.PipeReaderTupleCsv
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeReaderGoldstandardIdsCluster
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeReaderGoldstandardIdsPairs
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeReaderGoldstandardCluster
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeReaderGoldstandardIdToTuple
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeReaderGoldstandardPairs
import de.unihamburg.vsis.sddf.reading.PipeReaderOmitHead
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Id
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Ignore
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.Tuple

// analyse
import de.unihamburg.vsis.sddf.indexing.PipeAnalyseIndexer
import de.unihamburg.vsis.sddf.clustering.PipeAnalyseClustering
import de.unihamburg.vsis.sddf.classification.PipeAnalyseClassificationTraining
import de.unihamburg.vsis.sddf.classification.PipeAnalyseClassification
import de.unihamburg.vsis.sddf.reading.corpus.PipeAnalyseCorpus
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeAnalyseGoldstandard

// preprocessing
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorMerger
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorRemoveRegex
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorSplitter
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorToLower
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorTrim

// blocking
import de.unihamburg.vsis.sddf.indexing.blocking.PipeBlockerSortedNeighborhood
import de.unihamburg.vsis.sddf.indexing.blocking.PipeBlockerStandard
import de.unihamburg.vsis.sddf.indexing.blocking.PipeBlockerSuffixArray
import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilderBasic

// indexing
import de.unihamburg.vsis.sddf.indexing.PipeIndexerSortedNeighborhood
import de.unihamburg.vsis.sddf.indexing.PipeIndexerStandard
import de.unihamburg.vsis.sddf.indexing.PipeIndexerSuffixArray

// similarity
import de.unihamburg.vsis.sddf.similarity.PipeSimilarity

// classification
import de.unihamburg.vsis.sddf.classification.PipeClassificationDecisionTree
import de.unihamburg.vsis.sddf.classification.PipeClassificationSvm
import de.unihamburg.vsis.sddf.classification.PipeClassificationNaiveBayes
import de.unihamburg.vsis.sddf.classification.PipeClassificationThreshold
import de.unihamburg.vsis.sddf.classification.PipeClassificationTrainingDataGenerator

// clustering
import de.unihamburg.vsis.sddf.clustering.PipeClusteringStrongestPath
import de.unihamburg.vsis.sddf.clustering.PipeClusteringTransitiveClosure

// printing
import de.unihamburg.vsis.sddf.classification.PipePrintHeadFalsePositives
import de.unihamburg.vsis.sddf.classification.PipePrintHeadFalseNegatives
import de.unihamburg.vsis.sddf.classification.PipePrintSampleFalseNegatives
import de.unihamburg.vsis.sddf.classification.PipePrintSampleFalsePositives
import de.unihamburg.vsis.sddf.reading.corpus.PipePrintHeadCorpus
import de.unihamburg.vsis.sddf.reading.corpus.PipePrintSampleCorpus
import de.unihamburg.vsis.sddf.reading.goldstandard.PipePrintHeadGoldstandard
import de.unihamburg.vsis.sddf.reading.goldstandard.PipePrintSampleGoldstandard
import de.unihamburg.vsis.sddf.print.PipePrintHead

// optimize
import de.unihamburg.vsis.sddf.pipe.optimize._

// writing
import de.unihamburg.vsis.sddf.writing.PipeWriterTuple
import de.unihamburg.vsis.sddf.writing.PipeWriterTupleCluster
import de.unihamburg.vsis.sddf.writing.PipeWriterTupleClusterActualDate
import de.unihamburg.vsis.sddf.writing.PipeWriterTuplePairs

// convert
import de.unihamburg.vsis.sddf.convert.PipeConvertClusterToPair
import de.unihamburg.vsis.sddf.convert.PipeConvertSetToPair

