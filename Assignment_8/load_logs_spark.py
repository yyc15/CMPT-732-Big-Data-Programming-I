from pyspark.sql import SparkSession, functions, types
import sys
import re, string
import uuid
import datetime
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')


def web_log_filter(line):
    web_log_str = re.match(line_re, line)

    if web_log_str:
        id = str(uuid.uuid1())
        host = web_log_str.group(1)
        date_time = datetime.datetime.strptime(web_log_str.group(2), '%d/%b/%Y:%H:%M:%S')
        path = web_log_str.group(3)
        number_of_bytes = int(web_log_str.group(4))

        return id, host, date_time, path, number_of_bytes


def remove_empty(value):
    if value is not None:
        return value


def main(inputs, keyspace, table):

    web_log_schema = types.StructType([
        types.StructField('id', types.StringType()),
        types.StructField('host', types.StringType()),
        types.StructField('datetime', types.TimestampType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.IntegerType()),
    ])
    text = sc.textFile(inputs).repartition(150)  # 80: 4m35s ; 100: 4m26s ; 150: 4m13s ; 200: 4m28s
    web_log_rdd = text.map(web_log_filter).filter(remove_empty)
    web_log_df = spark.createDataFrame(web_log_rdd, web_log_schema)
    web_log_df.write.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace).option("confirm.truncate", "true").mode("overwrite").save()


if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/nasa-logs-2"
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('load logs spark') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, keyspace, table)
