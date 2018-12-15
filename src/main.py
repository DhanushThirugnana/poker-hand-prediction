import sys

from preprocessor import tokenize, not_contains_null
from utils import remove_least_quality_rdd
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def submit_spark_app(master, appName, host, port, interval):
    ec_spark_context = SparkContext(master, appName)
    ec_spark_streaming_context = StreamingContext(ec_spark_context, batchDuration=interval)

    batch_of_poker_hands_rdd = ec_spark_streaming_context.socketTextStream(host, port)
    preprocessed_batch_of_poker_hands_rdd = batch_of_poker_hands_rdd.map(tokenize).filter(not_contains_null)

    preprocessed_batch_of_poker_hands_rdd.count().pprint()

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