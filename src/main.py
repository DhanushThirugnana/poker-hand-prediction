import sys

from preprocessor import tokenize, notContainsNull
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def submit_spark_app(master, appName, host, port, interval):
    ec_spark_context = SparkContext(master, appName)
    ec_spark_streaming_context = StreamingContext(ec_spark_context, batchDuration=interval)

    lines = ec_spark_streaming_context.socketTextStream(host, port)
    words = lines.map(tokenize).filter(notContainsNull)

    words.count().pprint()

    ec_spark_streaming_context.start()
    ec_spark_streaming_context.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise Exception("Insufficient Arguments: python3 main.py <port> <stream_interval>")

    APPNAME = "HouseholdElectricityConsumption"
    HOSTNAME = "localhost"
    MASTER = "local[4]"
    PORT = int(sys.argv[1])
    STREAM_INTERVAL = int(sys.argv[2])

    submit_spark_app(MASTER, APPNAME, HOSTNAME, PORT, STREAM_INTERVAL)
