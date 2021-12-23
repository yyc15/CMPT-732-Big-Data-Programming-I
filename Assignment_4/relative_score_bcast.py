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


def get_relative_score(bcast_object, comments):
    subreddit = comments["subreddit"]
    score = comments["score"]
    average = bcast_object.value[subreddit]
    relative_score = score/average
    author = comments["author"]
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
    reddit_average_object = dict(reddit_average.collect())
    # print('length of reddit_average.collect(): ', len(reddit_average.collect()))
    # size of the data in reddit-1 : 5 ; reddit-2: 8 ; reddit-3: 5
    reddit_average_broadcast_object = sc.broadcast(reddit_average_object)
    commentdata = text.map(json.loads).cache()
    broadcast_join = commentdata.map(lambda c: get_relative_score(reddit_average_broadcast_object, c))
    relative_score = broadcast_join.sortBy(get_key, ascending=False)
    relative_score.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit score broadcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/reddit-2"
    # output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-1"
    main(inputs, output)
