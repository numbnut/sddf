package de.unihamburg.vsis.sddf.pipe.context

import de.unihamburg.vsis.sddf.visualisation.model.BlockingModel
import de.unihamburg.vsis.sddf.visualisation.model.ClusterModel
import de.unihamburg.vsis.sddf.visualisation.model.ClassificationModel
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardClusterModel
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardModel
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModel
import de.unihamburg.vsis.sddf.visualisation.model.ReadingModel
import de.unihamburg.vsis.sddf.visualisation.model.TrainingSetModel

trait ResultContext {

  // mutable Options !!!
  var blockingModel: Option[BlockingModel] = None
  var indexingModel: Option[IndexingModel] = None
  var clusterModel: Option[ClusterModel] = None
  var classificationModel: Option[ClassificationModel] = None
  var goldstandardModel: Option[GoldstandardModel] = None
  var goldstandardModelCluster: Option[GoldstandardClusterModel] = None
  var readingModel: Option[ReadingModel] = None
  var trainingSetModel: Option[TrainingSetModel] = None

}