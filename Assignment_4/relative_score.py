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


def get_average(kv):
    k, v = kv
    reddit_count = v[0]
    reddit_sum = v[1]
    reddit_average = reddit_sum / reddit_count
    if reddit_average > 0:
        return k, reddit_average


def get_relative_score(input_values):
    score = input_values[1][0]["score"]
    average = input_values[1][1]
    relative_score = score/average
    author = input_values[1][0]["author"]
    return relative_score, author


def get_key(kv):
    return kv[0]


# SparkSkeleton
def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    json_records = text.map(reddit_json_mapper).cache()
    record_pairs = json_records.reduceByKey(add_pairs)
    reddit_average = record_pairs.map(get_average)
    commentdata = text.map(json.loads).cache()
    commentbysub = commentdata.map(lambda c: (c["subreddit"], c))
    join_average = commentbysub.join(reddit_average)
    relative_score = join_average.map(get_relative_score).sortBy(get_key, ascending=False)
    relative_score.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/reddit-2"
    # output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-1"
    main(inputs, output)
