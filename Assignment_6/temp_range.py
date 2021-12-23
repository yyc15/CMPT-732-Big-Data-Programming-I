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
    weather_filter = weather.filter(
        (weather['qflag'].isNull()) &
        ((weather['observation'] == 'TMAX') | (weather['observation'] == 'TMIN'))
    ).groupBy(
        weather['date'],
        weather['station']
    ).agg(
        (functions.max(weather['value']) - functions.min(weather['value'])).alias("range")
    ).cache()

    max_temperature_range = weather_filter.groupBy(
        weather_filter['date'].alias("max_date")
    ).agg(
        (functions.max(weather_filter['range'])).alias("max_range")
    )

    #  join them back together and subtract to get the temperature range
    result_temperature_range = weather_filter.join(
            max_temperature_range,
            (weather_filter['date'] == max_temperature_range['max_date'])
            & (weather_filter['range'] == max_temperature_range['max_range'])
    ).select(
        weather_filter['date'],
        weather_filter['station'],
        (weather_filter['range']/10).alias("range")
    ).sort(
        weather_filter['date'],
        weather_filter['station']
    )

    result_temperature_range.coalesce(1).write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/weather-1"
    # output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-temp-range"
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
