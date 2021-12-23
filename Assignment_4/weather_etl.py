import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions, types


def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])

    weather = spark.read.csv(inputs, schema=observation_schema)
    qflag_filter = weather.filter(weather.qflag.isNull())
    station_filter = qflag_filter.filter(qflag_filter.station.startswith('CA'))
    observation_filter = station_filter.filter(station_filter.observation == 'TMAX')
    temperature_value = observation_filter.withColumn('tmax', observation_filter.value / 10)
    etl_data = temperature_value.select('station', 'date', 'tmax')
    etl_data.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/weather-1"
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
