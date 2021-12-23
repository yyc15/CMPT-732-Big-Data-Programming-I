import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # define StringIndexer, classifier, evaluator
    word_indexer = StringIndexer(inputCol="word", outputCol="label", handleInvalid="error",
                                 stringOrderType="frequencyDesc")
    classifier = MultilayerPerceptronClassifier(maxIter=200, layers=[3, 30, 11], blockSize=2, seed=200)
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")

    # TODO: create a pipeline to predict RGB colours -> word
    rgb_assembler = VectorAssembler(inputCols=["R", "B", "G"], outputCol="features")
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)

    # TODO: create an evaluator and score the validation data
    rbg_result = rgb_model.transform(validation)
    rbg_score = evaluator.evaluate(rbg_result)
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('*********************RBG**********************')
    print('Validation score for RGB model:', rbg_score)
    print('**********************************************')

    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=["labL", "labA", "labB"], outputCol="features")
    lab_pipeline = Pipeline(stages=[sqlTrans, lab_assembler, word_indexer, classifier])
    lab_model = lab_pipeline.fit(train)
    lab_result = lab_model.transform(validation)
    lab_score = evaluator.evaluate(lab_result)
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('*********************LAB**********************')
    print('Validation score for LAB model:', lab_score)
    print('**********************************************')


if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('colour prediction').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)
