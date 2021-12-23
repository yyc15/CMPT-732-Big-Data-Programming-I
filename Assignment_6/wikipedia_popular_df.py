import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions, types

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    new_path = path.split("/")
    filename = new_path[-1]  # pagecounts-20160801-120000
    datetime = filename[11:22]  # 20160801-12
    return datetime


def main(inputs, output):
    wiki_popular_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views', types.LongType())
    ])

    wiki_data = spark.read.csv(inputs, schema=wiki_popular_schema, sep=" ")
    wiki_data = wiki_data.withColumn("datetime", path_to_hour(functions.input_file_name()))
    wiki_filter = wiki_data.filter(
        (wiki_data['language'] == "en")
        & (wiki_data['title'] != "Main_Page")
        & (wiki_data['title'].startswith("Special:") == False)
    )
    #   view data structure: hour, max_view
    wiki_max_view = wiki_filter.groupby(
        wiki_filter['datetime'].alias("hour")
    ).agg(
        functions.max(wiki_filter['views']).alias("max_views")
    ).cache()


    # with broadcast hints
    # wiki_max_join = wiki_filter.join(functions.broadcast(wiki_max_view), (wiki_max_view['hour'] == wiki_filter['datetime'])).where(wiki_max_view['max_views'] == wiki_filter['views'])
    # wiki_df = wiki_view_join.select(wiki_view_join['hour'], wiki_view_join['title'], wiki_view_join['views']).sort(wiki_view_join['hour'])

    # without broadcast hints
    wiki_df = wiki_filter.join(
        wiki_max_view,
        (wiki_max_view['hour'] == wiki_filter['datetime'])
        & (wiki_max_view['max_views'] == wiki_filter['views'])
    ).select(
        wiki_max_view['hour'], wiki_filter['title'], wiki_filter['views']
    ).sort(
        wiki_max_view['hour'],
        wiki_filter['title']
    )

    wiki_df.coalesce(1).write.json(output, mode='overwrite')
    # wiki_df.explain()


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/pagecounts-0"
    # output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-wiki-df"
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
