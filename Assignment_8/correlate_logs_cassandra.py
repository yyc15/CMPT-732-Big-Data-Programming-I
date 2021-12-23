from pyspark.sql import SparkSession, functions, types
import sys
import math
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(keyspace, table):
    web_log_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace).load()
    hostname_web_log_df = web_log_df.groupBy(
        web_log_df['host']
    ).agg(
        functions.count(web_log_df['path']).alias('x'),
        functions.sum(web_log_df['bytes']).alias('y')
    )
    reform_df = hostname_web_log_df.withColumn(
        '1', functions.lit(1)
    ).withColumn(
        'x^2', hostname_web_log_df['x'] ** 2
    ).withColumn(
        'y^2', hostname_web_log_df['y'] ** 2
    ).withColumn(
        'xy', (hostname_web_log_df['x'] * hostname_web_log_df['y'])
    ).cache()
    # rearrange df columns and add (1, x, x^2, y, y^2, xy) to get the six sums

    six_sums = reform_df.select(
        functions.sum(reform_df['1']).alias('n'),
        functions.sum(reform_df['x']).alias('sum_x'),
        functions.sum(reform_df['x^2']).alias('sum_x^2'),
        functions.sum(reform_df['y']).alias('sum_y'),
        functions.sum(reform_df['y^2']).alias('sum_y^2'),
        functions.sum(reform_df['xy']).alias('sum_xy')
    ).collect()

    # Calculate the final value of r
    six_sums_list = six_sums[0]
    # n
    n = six_sums_list['n']
    print('n = ' + str(n))
    # sum x
    sum_x = six_sums_list['sum_x']
    print('sum x = ' + str(sum_x))
    # sum x^2
    sum_x2 = six_sums_list['sum_x^2']
    print('sum x^2 = ' + str(sum_x2))
    # sum y
    sum_y = six_sums_list['sum_y']
    print('sum y = ' + str(sum_y))
    # sum y^2
    sum_y2 = six_sums_list['sum_y^2']
    print('sum y^2 = ' + str(sum_y2))
    # sum xy
    sum_xy = six_sums_list['sum_xy']
    print('sum xy = ' + str(sum_xy))
    numerator = ((n * sum_xy) - (sum_x * sum_y))
    print('numerator= ' + str(numerator))
    denominator = (math.sqrt((n * sum_x2) - (sum_x ** 2))) * (math.sqrt((n * sum_y2) - (sum_y ** 2)))
    print('denominator= ' + str(denominator))
    r = numerator / denominator
    r2 = r ** 2
    print('r = ' + str(r))
    print('r^2 = ' + str(r2))


if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('correlate logs cassandra') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace, table)

