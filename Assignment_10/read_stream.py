from pyspark.sql import SparkSession, functions, types
import sys
import re
from kafka import KafkaConsumer

def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    new_values = functions.split(values['value'], ' ')
    x = new_values.getItem(0)
    y = new_values.getItem(1)

    values_df = values.withColumn('1', functions.lit(1)).withColumn('x', x)\
        .withColumn('y', y).withColumn('xy', x*y).withColumn('x^2', x**2)
    sum_values_df = values_df.agg(
                            functions.sum(values_df['1']).alias('n'),
                            functions.sum(values_df['x']).alias('sum_x'),
                            functions.sum(values_df['y']).alias('sum_y'),
                            functions.sum(values_df['xy']).alias('sum_xy'),
                            functions.sum(values_df['x^2']).alias('sum_x^2'),
                        )
    # n
    n = sum_values_df['n']
    # print('n = ' + str(n))
    # sum x
    sum_x = sum_values_df['sum_x']
    # print('sum x = ' + str(sum_x))
    # sum y
    sum_y = sum_values_df['sum_y']
    # print('sum y = ' + str(sum_y))
    # sum xy
    sum_xy = sum_values_df['sum_xy']
    # print('sum xy = ' + str(sum_xy))
    # sum x^2
    sum_x2 = sum_values_df['sum_x^2']
    # print('sum x^2 = ' + str(sum_x2))

    slope = (sum_xy - ((1/n)*sum_x*sum_y))/(sum_x2-((1/n)*(sum_x**2)))
    intercept = (sum_y/n) - slope*(sum_x/n)
    # print('slope = ' + str(slope))
    # print('intercept = ' + str(intercept))

    calculation_df = sum_values_df.withColumn('slope', slope).withColumn('intercept', intercept)
    streaming_df = calculation_df.select(calculation_df['slope'], calculation_df['intercept'])
    stream = streaming_df.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(600)


if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('read stream').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(topic)
