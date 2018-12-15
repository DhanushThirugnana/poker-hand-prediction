import sys

from preprocessor import tokenize, not_contains_null
from utils import remove_least_quality_rdd
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def submit_spark_app(master, appName, host, port, interval, path_test_data, rdd_list):
    ec_spark_context = SparkContext(master, appName)
    ec_spark_streaming_context = StreamingContext(ec_spark_context, batchDuration=interval)

    # Read the test data set.
    test_poker_hands_rdd = ec_spark_context.textFile(path_test_data).map(tokenize).filter(not_contains_null)

    # TODO: Preprocess the test dataset.
    preprocessed_test_poker_hands_rdd = test_poker_hands_rdd # Instead of simple assignment, preprocess and assign.

    # Split the features and class columns from the previous preprocessed RDD.
    test_poker_hands_features_rdd = preprocessed_test_poker_hands_rdd.map(lambda x: x.split(",")[0:-1])
    test_poker_hands_class_rdd = preprocessed_test_poker_hands_rdd.map(lambda x: x.split(",")[-1])

    # Read data from stream.
    batch_of_poker_hands_rdd = ec_spark_streaming_context.socketTextStream(host, port)
    preprocessed_batch_of_poker_hands_rdd = batch_of_poker_hands_rdd.map(tokenize).filter(not_contains_null)

    # Append the preprocessed rdd to the global list.
    rdd_list.append(preprocessed_batch_of_poker_hands_rdd)

    # TODO: Create multiple models using the k-folds of rdds obtained from our list
    # TODO: Find the accuracy for each model using the test data.
    # TODO: Report the highest/average accuracy

    # TODO : Get the RDD list vs accuracy tuple here.
    # rddlist_accuracy_rdd =

    # TODO : Remove the least quality RDD from the list of RDDs.
    # remove_least_quality_rdd(rdd_list, rddlist_accuracy_rdd)
    preprocessed_batch_of_poker_hands_rdd.count().pprint()

    ec_spark_streaming_context.start()
    ec_spark_streaming_context.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) < 4:
        raise Exception("Insufficient Arguments: python3 main.py <port> <stream_interval> <path_to_test_data>")

    APPNAME = "HouseholdElectricityConsumption"
    HOSTNAME = "localhost"
    MASTER = "local[4]"
    PORT = int(sys.argv[1])
    STREAM_INTERVAL = int(sys.argv[2])
    TEST_DATA_FILE_PATH = sys.argv[3]

    submit_spark_app(MASTER, APPNAME, HOSTNAME, PORT, STREAM_INTERVAL, TEST_DATA_FILE_PATH, [])