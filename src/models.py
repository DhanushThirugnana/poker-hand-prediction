from pyspark.mllib.tree import RandomForest
from pyspark.mllib.tree import GradientBoostedTrees
import numpy as np

def precreate_models(train_data):
    models = list()

    for depth in range(9, 10):
        for num_trees in range(4, 10, 3):
            for impurity in ['entropy']: # ['gini', 'entropy']
                for feature in ['onethird']: # ['auto', 'all', 'sqrt', 'log2', 'onethird']
                    models.append(RandomForest.trainClassifier(train_data, numClasses=10, categoricalFeaturesInfo={}, numTrees=num_trees, featureSubsetStrategy=feature, impurity=impurity, maxDepth=depth, maxBins=32))

    for iters in range(9, 10):
        for rate in np.linspace(0.1,1,2):
            for depth in range(9, 10):
                for loss in ['leastSquaresError']: # ['logLoss', 'leastSquaresError', 'leastAbsoluteError']
                    models.append(GradientBoostedTrees.trainClassifier(train_data, categoricalFeaturesInfo={}, loss=loss, numIterations=iters, learningRate=rate, maxDepth=depth))

    return models

def test_model(model, test_data_rdd):
    predictions = model.predict(test_data_rdd.map(lambda x: x.features))
    labels_and_predictions = test_data_rdd.map(lambda lp: lp.label).zip(predictions)
    error = labels_and_predictions.filter(lambda lp: lp[0] != lp[1]).count() / float(test_data_rdd.count())
    return error