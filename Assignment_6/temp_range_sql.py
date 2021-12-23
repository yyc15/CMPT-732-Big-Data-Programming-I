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
    weather.createOrReplaceTempView("weather_view")

    weather_filter = spark.sql("""
            SELECT date, station, value FROM weather_view 
            WHERE qflag IS NULL 
            AND (observation =='TMAX' OR observation =='TMIN')
        """)
    weather_filter.createOrReplaceTempView("weather_filter_view")

    temperature_range = spark.sql("""
            SELECT date, station, MAX(value) - MIN(value) AS range 
            FROM weather_filter_view 
            GROUP BY date, station
        """)
    temperature_range.createOrReplaceTempView("temperature_range_view")

    max_temperature_range = spark.sql("""
            SELECT date, MAX(range) AS max_range 
            FROM temperature_range_view 
            GROUP BY date
        """).cache()
    max_temperature_range.createOrReplaceTempView("max_temperature_range_view")

    result_temp_range = spark.sql("""
            SELECT temperature_range_view.date, temperature_range_view.station, temperature_range_view.range/10 AS range
            FROM temperature_range_view 
            JOIN max_temperature_range_view 
            ON temperature_range_view.date = max_temperature_range_view.date 
            WHERE temperature_range_view.range = max_temperature_range_view.max_range
            ORDER BY date, station
        """)

    result_temp_range.coalesce(1).write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/weather-1"
    # output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-temp-range"
    spark = SparkSession.builder.appName('temp range sql').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
