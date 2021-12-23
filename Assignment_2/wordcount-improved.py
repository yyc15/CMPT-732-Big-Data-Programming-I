from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

# inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/wordcount-1"
# output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-1"

conf = SparkConf().setAppName('wordcount improved')
sc = SparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

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


text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordcount = words.reduceByKey(add)
outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
