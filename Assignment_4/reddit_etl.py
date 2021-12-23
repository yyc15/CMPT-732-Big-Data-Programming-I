from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def reddit_extract(json_file):
    record = json.loads(json_file)
    subreddit = record.get("subreddit")
    score = record.get("score")
    author = record.get("author")
    return subreddit, score, author


def reddit_filter(input_values):
    if "e" in input_values[0]:
        return True
    else:
        return False


def check_positive(input_values):
    if input_values[1] > 0:
        return True


def check_negative(input_values):
    if input_values[1] <= 0:
        return True


# SparkSkeleton
def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    extract_records = text.map(reddit_extract)
    filter_records = extract_records.filter(reddit_filter).cache()
    positive_records = filter_records.filter(check_positive)
    negative_records = filter_records.filter(check_negative)
    positive_records.map(json.dumps).saveAsTextFile(output + '/positive')
    negative_records.map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/reddit-2"
    # output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-1"
    main(inputs, output)
