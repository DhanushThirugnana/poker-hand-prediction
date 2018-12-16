# import matplotlib.pyplot as plt
import pandas as pd

training_data_path = '/Users/manvapradhan/PycharmProjects/big-data-project/data/poker-train.csv'
testing_data_path = '/Users/manvapradhan/PycharmProjects/big-data-project/data/poker-test.csv'

training_data = pd.read_csv(training_data_path)
testing_data = pd.read_csv(testing_data_path)

training_data.columns = ['s1', 'c1', 's2', 'c2', 's3', 'c3', 's4', 'c4', 's5', 'c5', 'class']
testing_data.columns = ['s1', 'c1', 's2', 'c2', 's3', 'c3', 's4', 'c4', 's5', 'c5', 'class']

# print(pd.DataFrame.transpose(training_data.describe()))
# print(pd.DataFrame.transpose(testing_data.describe()))
#
# print(training_data.shape)
# print(testing_data.shape)

for i in training_data.columns:
    print(training_data.groupby(i).count())
