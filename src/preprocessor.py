import pandas as pd
from pyspark.mllib.regression import LabeledPoint
from utils import cast_to_float


def tokenize(line):
    tokens = line.split(",")
    for i in range(0, len(tokens)):
        tokens[i] = cast_to_float(tokens[i])
    return tokens


def not_contains_null(rowAsArr):
    for element in rowAsArr:
        if element is None:
            return False
    return True


def read_data(training_data, test_data):
    df_train = pd.read_csv(training_data, header=None)
    df_test = pd.read_csv(test_data, header=None)
    return df_train, df_test


def drop_null_duplicates(train, test):
    print('Unprocessed Training Shape: ' + str(train.shape))
    print('Unprocessed Testing Shape: ' + str(test.shape))
    df_train = train.drop_duplicates()
    df_test = test.drop_duplicates()
    df_train = df_train.dropna()
    df_test = df_test.dropna()
    print('Preprocessed Training Shape: ' + str(df_train.shape))
    print('Preprocessed Testing Shape: ' + str(df_test.shape))
    return train, test


def get_preprocessed_data(path_train, path_test):
    train, test = read_data(path_train, path_test)
    print(pd.DataFrame.transpose(train.describe()))
    print(pd.DataFrame.transpose(test.describe()))
    train, test = drop_null_duplicates(train, test)
    train.columns = ['s1', 'c1', 's2', 'c2', 's3', 'c3', 's4', 'c4', 's5', 'c5', 'class']
    test.columns = ['s1', 'c1', 's2', 'c2', 's3', 'c3', 's4', 'c4', 's5', 'c5', 'class']
    return train, test


def make_labeled_point(data):
    return LabeledPoint(data[len(data) - 1], data[0:len(data) - 1])
