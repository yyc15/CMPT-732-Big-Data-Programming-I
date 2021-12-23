from pyspark import SparkConf, SparkContext
import sys
import random

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def random_iterations(n):
    random.seed()
    iterations = 0
    for i in range(len(n)):
        random_sum = 0.0
        while random_sum < 1:
            random_sum += random.random()  # [random double 0 to 1] Return the next random floating point number in the range [0.0, 1.0)
            iterations += 1
    yield iterations


def add(x, y):
    return x + y


def main(inputs):
    # main logic starts here
    samples = int(inputs)
    rdd = sc.parallelize(range(samples), numSlices=30)  # n = 100000000  1: 1m3.822s  5: 43.540s  6:0m42.898s  10:0m39.993s  30:0m39.672s 100:43.640s
    rdd_map = rdd.mapPartitions(random_iterations)
    total_iterations = rdd_map.reduce(add)  # only return one sum
    # output
    print("result: " + str(total_iterations / samples))


if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    # inputs = 1000000
    main(inputs)
