import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn
from utils import cast_to_float
from pyspark.mllib.regression import LabeledPoint


def tokenize(line):
    tokens = line.split(",")
    # tokens[1] = datetime.datetime.strptime(tokens[1], '%H:%M:%S').time()
    for i in range(0, len(tokens)):
        tokens[i] = cast_to_float(tokens[i])
    return tokens


def not_contains_null(rowAsArr):
    for element in rowAsArr:
        if element is None:
            return False
    return True


def add_active_consumption(row):
    global_active_power = row[2]
    sub_metering_1 = row[6]
    sub_metering_2 = row[7]
    sub_metering_3 = row[8]

    active_consumption = global_active_power * 1000 / 60 - sub_metering_1 - sub_metering_2 - sub_metering_3
    row.append(active_consumption)

    row.append(0)
    return row


def remove_negative_consumption(row):
    if row[9] < 0:
        return False
    return True


def print_plots(df):
    for attr in list(df.columns.values):
        df[attr].plot.hist()
        print(plt.show())
    seaborn.pairplot(df, hue='class')
    print(plt.show())


def pre_processing(data):
    encoding = [0 for i in range(len(data[0]))]
    for col in range(len(data[0])):
        targets = data[:, col] - 1
        if col % 2 == 0:
            encoding[col] = np.zeros((len(data), 4))
        else:
            encoding[col] = np.zeros((len(data), 13))
        encoding[col][np.arange(len(data)), targets] = 1
        encoding[col] = pd.DataFrame(encoding[col])
    return pd.concat(encoding, axis=1).values


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
    # print_plots(train)
    # print_plots(test)
    train, test = drop_null_duplicates(train, test)
    train.columns = ['s1', 'c1', 's2', 'c2', 's3', 'c3', 's4', 'c4', 's5', 'c5', 'class']
    test.columns = ['s1', 'c1', 's2', 'c2', 's3', 'c3', 's4', 'c4', 's5', 'c5', 'class']
    # y_train = train.iloc[:, -1]
    # y_test = test.iloc[:, -1]
    # x_train = pd.DataFrame(pre_processing(train.iloc[:, :-1].values))
    # x_test = pd.DataFrame(pre_processing(test.iloc[:, :-1].values))
    # x_train["class"] = y_train
    # x_test["class"] = y_test
    # print('Pre-processing done.')
    # return x_train, x_test
    return train, test

# get_preprocessed_data('poker-train.csv', 'poker-test.csv', 'clean-train', 'clean-test')

def make_labeled_point(data):
    return LabeledPoint(data[len(data)-1], data[0:len(data)-1])