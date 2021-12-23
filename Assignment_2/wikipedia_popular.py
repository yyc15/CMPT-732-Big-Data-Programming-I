from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

# inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/pagecounts-with-time-1"
# output = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/output-2"

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

""""
Read the input file(s) in as lines (as in the word count).
Break each line up into a tuple of five things (by splitting around spaces). This would be a good time to convert he view count to an integer. (.map())
Remove the records we don't want to consider. (.filter())
Create an RDD of key-value pairs. (.map())
Reduce to find the max value for each key. (.reduceByKey())
Sort so the records are sorted by key. (.sortBy())
Save as text output (see note below).
"""


def wikipedia_mapper(filename):
    elements = filename.split()
    return tuple(elements)


def wikipedia_filter(elements):
    language = elements[1]
    title = elements[2]
    if language == "en" and title != "Main_Page" and not title.startswith('Special:'):
        return elements


def get_values(elements):
    datetime = elements[0]
    title = elements[2]
    view = int(elements[3])
    return datetime, (view, title)


def get_max_count(x, y):
    return max(x, y)


def get_key(kv):
    return kv[0]


def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])


text = sc.textFile(inputs)
elements = text.map(wikipedia_mapper)
result_filter = elements.filter(wikipedia_filter)
filter_map = result_filter.map(get_values)
max_count = filter_map.reduceByKey(get_max_count)
max_count.sortBy(get_key).map(tab_separated).saveAsTextFile(output)


