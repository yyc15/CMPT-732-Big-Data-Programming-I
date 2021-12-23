from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# Assignment 2
# inputs = sys.argv[1]
# output = sys.argv[2]

# inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/wordcount-1"
# output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-1"

# conf = SparkConf().setAppName('wordcount improved')
# sc = SparkContext(conf=conf)


wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))


def words_once(line):
    words_str = wordsep.split(line)
    words_filtered = filter(lambda x: len(x) > 0, words_str)
    for w in words_filtered:
        yield w.lower(), 1


def add(x, y):
    return x + y


def get_key(kv):
    return kv[0]


def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

# Assignment 2
"""
text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordcount = words.reduceByKey(add)
outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
"""


# Assignment 3
# SparkSkeleton
def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    text = text.repartition(25) # 100: 3.2 mins 75: 2.6mins 50:2.3 mins 25: 2.3mins 18: 2.6 mins
    words = text.flatMap(words_once)
    wordcount = words.reduceByKey(add)
    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/wordcount-1"
    # output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-1"
    main(inputs, output)