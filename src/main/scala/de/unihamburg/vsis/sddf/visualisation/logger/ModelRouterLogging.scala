package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.config.Config
import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.visualisation.ModelRouter
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable
import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable
import de.unihamburg.vsis.sddf.visualisation.model.BlockingModel
import de.unihamburg.vsis.sddf.visualisation.model.ClusterModel
import de.unihamburg.vsis.sddf.visualisation.model.ClassificationModel
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardClusterModel
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardModel
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModelExtended
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModel
import de.unihamburg.vsis.sddf.visualisation.model.ReadingModel
import de.unihamburg.vsis.sddf.visualisation.model.TrainingSetModel

object ModelRouterLogging extends ModelRouter with Logging {
  def submitModel(model: Analysable): Unit = {
    model match {
      case mdl: IndexingModelExtended   => IndexingOutputterExtended.logResults(mdl)
      case mdl: IndexingModel            => IndexingOutputter.logResults(mdl)
      case mdl: BlockingModel            => BlockingOutputter.logResults(mdl)
      case mdl: ClusterModel             => ClusterOutputter.logResults(mdl)
      case mdl: ClassificationModel      => ClassificationOutputter.logResults(mdl)
      case mdl: GoldstandardModel        => GoldstandardOutputter.logResults(mdl)
      case mdl: GoldstandardClusterModel => GoldstandardClusterOutputter.logResults(mdl)
      case mdl: ReadingModel             => ReadingOutputter.logResults(mdl)
      case mdl: TrainingSetModel         => TrainingSetOutputter.logResults(mdl)
      case mdl: AlgoAnalysable           => AlgoOutputter.logResults(mdl)
      case mdl: BasicAnalysable          => BasicOutputter.logResults(mdl)
      case _                             => log.warn("No Outputter for model type " + model.getClass)
    }
  }
}