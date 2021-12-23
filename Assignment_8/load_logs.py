from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
import sys
import re, string
import gzip
import os
import uuid
import datetime

cluster = Cluster(['node1.local', 'node2.local'])


def main(inputs, keyspace, table):
    session = cluster.connect(keyspace)
    session.execute("CREATE TABLE IF NOT EXISTS  "
                    + table + "(id UUID, host TEXT, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY(host, id));")
    batch = BatchStatement()
    count = 0
    batch_size = 400
    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
                web_log_str = re.match(line_re, line)
                if web_log_str:
                    id = uuid.uuid1()
                    #print(id)
                    host = web_log_str.group(1)
                    #print(host)
                    date_time = datetime.datetime.strptime(web_log_str.group(2), '%d/%b/%Y:%H:%M:%S')
                    #print(date_time)
                    path = web_log_str.group(3)
                    #print(path)
                    number_of_bytes = int(web_log_str.group(4))
                    #print(number_of_bytes)
                    batch.add(SimpleStatement(
                        "INSERT INTO " + table + " (id, host, datetime, path, bytes) VALUES (%s, %s, %s, %s, %s)"), (
                              id, host, date_time, path, number_of_bytes
                        )
                    )
                    count += 1
                    if count == batch_size:
                        session.execute(batch)
                        count = 0  # reset count
                        batch.clear()  # reset batch

    session.execute(batch)  # cater those batch size < batch_size
    batch.clear()

    # Check
    #rows = session.execute('SELECT sum(bytes) AS total_number_of_bytes FROM nasalogs')
    #print(rows)


if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    # inputs = "/Users/kevanichow/PycharmProjects/CMPT732Assignment/nasa-logs-2"
    main(inputs, keyspace, table)

