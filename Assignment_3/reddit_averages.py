from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def reddit_json_mapper(json_file):
    record = json.loads(json_file)
    subreddit = record.get("subreddit")
    score = record.get("score")
    score_int = int(score)
    one = 1
    return subreddit, (one, score_int)


def add_pairs(key_value1, key_value2):
    reddit_count = key_value1[0] + key_value2[0]
    reddit_sum = key_value1[1] + key_value2[1]
    return reddit_count, reddit_sum


def output_format(kv):
    k, v = kv
    reddit_count = v[0]
    reddit_sum = v[1]
    reddit_average = reddit_sum / reddit_count
    return json.dumps([k, reddit_average])


# SparkSkeleton
def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    json_records = text.map(reddit_json_mapper)
    record_pairs = json_records.reduceByKey(add_pairs)
    reddit_average = record_pairs.map(output_format)
    reddit_average.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    #inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/reddit-0"
    #output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-1"
    main(inputs, output)
