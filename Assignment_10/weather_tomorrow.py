import sys
assert sys.version_info >= (3, 5)
import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import PipelineModel

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(inputs, model_file):
    # get the data
    data = spark.createDataFrame(inputs, schema=tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(data)
    predictions.show()

    df_row = predictions.first()
    prediction = df_row["prediction"]

    print('***********Predicted tmax tomorrow***********')
    print('Predicted tmax tomorrow:', prediction)
    print('*********************************************')


if __name__ == '__main__':
    model_file = sys.argv[1]
    today = datetime.date(2021, 11, 19)
    tomorrow = datetime.date(2021, 11, 20)
    latitude = 49.2771
    longitude = -122.9146
    elevation = 330.0
    tmax_today = 12.0
    tmax_tomorrow = 0.0
    inputs = [('SFU Southeast Classroom Block', today, latitude, longitude, elevation, tmax_today),
              ('SFU Southeast Classroom Block', tomorrow, latitude, longitude, elevation, tmax_tomorrow)]
    spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, model_file)
