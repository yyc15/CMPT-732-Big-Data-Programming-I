from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def get_path_value(graph):
    graph_list = graph.split(":")
    # print(graph_list)
    node = graph_list[0]
    out_edges = graph_list[1].split(" ")
    # print(node)
    # print(out_edges)
    for out_edge in out_edges:
        if len(out_edge) > 0:
            yield int(node), int(out_edge)


def main(inputs, output, source_node, destination_node):
    graph_edges_schema = types.StructType([
        types.StructField('node', types.IntegerType()),
        types.StructField('out_edge', types.IntegerType()),
    ])
    known_paths_schema = types.StructType([
        types.StructField('node', types.IntegerType()),
        types.StructField('source', types.IntegerType()),
        types.StructField('distance', types.IntegerType()),
    ])
    filename = '/links-simple-sorted.txt'
    text = sc.textFile(inputs + filename)
    path_val = text.flatMap(get_path_value)
    # path_val.take(10)
    # [(1, 3), (1, 5), (2, 4), (3, 2), (3, 5), (4, 3), (6, 1), (6, 5)]
    graph_edges_df = spark.createDataFrame(path_val, graph_edges_schema).cache()
    # graph_edges_df.show()
    known_paths_df = spark.createDataFrame([(source_node, 0, 0)], known_paths_schema)
    # known_paths_df.show()
    for i in range(6):
        path_df = known_paths_df.join(
                                    graph_edges_df
                                ).where(
                                    graph_edges_df['node'] == known_paths_df['node']
                                ).select(
                                    graph_edges_df['out_edge'].alias('node'),
                                    graph_edges_df['node'].alias('source'),
                                    (known_paths_df['distance'] + functions.lit(1)).alias('distance')
                                )
        # print("path_df.show()")
        # path_df.show()
        # remove the previous joined path
        short_path_df = path_df.subtract(known_paths_df)
        # print("short_path_df.show()")
        # short_path_df.show()
        known_paths_df = known_paths_df.unionAll(short_path_df)
        known_paths_rdd = known_paths_df.rdd
        known_paths_rdd.coalesce(1).saveAsTextFile(output + '/iter-' + str(i))
        # print('known_paths_df is:')
        # known_paths_df.show()
        if known_paths_rdd.lookup(destination_node):
            break

    initalpath = [destination_node]
    finalpath = []
    destination = destination_node
    if destination & destination != source_node:
        for i in range(6):
            path = known_paths_df.where(known_paths_df['node'] == destination).collect()
            # print('path: at ' + str(i))
            # print(path)
            destination = int(path[0][1])
            initalpath.append(destination)
            if destination == source_node:
                finalpath = list(reversed(initalpath))
                print('The shortest path is found:')
                print(finalpath)
                break
    else:
        print('Invalid Destination node OR  Destination node equals source node. Please try different value.')
        return

    finalpath = sc.parallelize(finalpath)
    finalpath.coalesce(1).saveAsTextFile(output + '/path')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source_node = int(sys.argv[3])
    destination_node = int(sys.argv[4])
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/graph-1"
    # output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-1"
    spark = SparkSession.builder.appName('shortest path').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, source_node, destination_node)
