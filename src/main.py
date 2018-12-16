import sys

from preprocessor import tokenize, not_contains_null, make_labeled_point
from utils import remove_least_quality_rdd, get_k_rdds_from_list, merge_k_rdds, get_merged_rdd
from models import precreate_models, test_model
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def submit_spark_app(host, port):
    # Read DStream from stream.
    batch_of_poker_hands_dstream = ec_spark_streaming_context.socketTextStream(host, port)
    preprocessed_batch_of_poker_hands_rdd = batch_of_poker_hands_dstream.map(tokenize).filter(not_contains_null).map(make_labeled_point)

    preprocessed_batch_of_poker_hands_rdd.foreachRDD(
        lambda rdd: process_rdd(rdd)
    )

def process_rdd(train_rdd):
    # Append the preprocessed rdd to the global list.
    RDD_LIST.append(ec_spark_context.parallelize(train_rdd.collect()))

    rddlist_accuracy_rdd = list()
    global_error = 1
    if (len(RDD_LIST) >= MINIMUM_RDD_LIST_LEN):
        for i in range(0, min(K, MINIMUM_RDD_LIST_LEN)):
            empty_rdd = ec_spark_context.parallelize([])
            rdd_subset = get_k_rdds_from_list(RDD_LIST, k=K)
            rdd = get_merged_rdd(empty_rdd, rdd_subset)
            min_error = min(list(map(lambda model: test_model(model, poker_hands_test_rdd), precreate_models(rdd))))
            rddlist_accuracy_rdd.append((rdd_subset,min_error))
            global_error = min(global_error, min_error)
        print("Error: "+ str(global_error))

    if (len(RDD_LIST) > MINIMUM_RDD_LIST_LEN):
        remove_least_quality_rdd(RDD_LIST, rddlist_accuracy_rdd, minimum_list_len=MINIMUM_RDD_LIST_LEN, optimizing_fn=max)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        raise Exception("Insufficient Arguments: python3 main.py <port> <stream_interval> <path_to_test_data>")

    APPNAME = "Poker Hand Classification"
    HOSTNAME = "localhost"
    MASTER = "local[4]"
    PORT = int(sys.argv[1])
    STREAM_INTERVAL = int(sys.argv[2])
    TEST_DATA_FILE_PATH = sys.argv[3]
    MINIMUM_RDD_LIST_LEN = 3
    K = 2
    RDD_LIST = list()
    ec_spark_context = SparkContext(MASTER, APPNAME)
    ec_spark_streaming_context = StreamingContext(ec_spark_context, batchDuration=STREAM_INTERVAL)

    # Read the test data set.
    poker_hands_test_rdd = ec_spark_context.textFile(TEST_DATA_FILE_PATH).map(tokenize).filter(not_contains_null).map(
        make_labeled_point)

    submit_spark_app(HOSTNAME, PORT)

    ec_spark_streaming_context.start()
    ec_spark_streaming_context.awaitTermination()