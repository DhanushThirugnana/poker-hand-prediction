# import matplotlib.pyplot as plt
import pandas as pd

training_data_path = '/Users/manvapradhan/PycharmProjects/big-data-project/data/poker-train.csv'
testing_data_path = '/Users/manvapradhan/PycharmProjects/big-data-project/data/poker-test.csv'

training_data = pd.read_csv(training_data_path)
testing_data = pd.read_csv(testing_data_path)

training_data.columns = ['s1', 'c1', 's2', 'c2', 's3', 'c3', 's4', 'c4', 's5', 'c5', 'class']
testing_data.columns = ['s1', 'c1', 's2', 'c2', 's3', 'c3', 's4', 'c4', 's5', 'c5', 'class']

print(pd.DataFrame.transpose(training_data.describe()))
print(pd.DataFrame.transpose(testing_data.describe()))
#
print(training_data.shape)
print(testing_data.shape)
#
training_count = {}
for i in training_data.columns:
    count = {}
    for x in training_data[i].values:
        if x not in count:
            count[x] = 1
        else:
            count[x] += 1

    # for key in sorted(count.items()):
    #     print(key)

    training_count[i] = count

testing_count = {}
for i in testing_data.columns:
    count = {}
    for x in testing_data[i].values:
        if x not in count:
            count[x] = 1
        else:
            count[x] += 1

    # for key in sorted(count.items()):
    #     print(key)

    testing_count[i] = count
#
print(training_count['class'])
print(testing_count['class'])

#
# for (i, j) in zip(training_count, testing_count):
#     for (x, y) in zip(training_count[i], testing_count[j]):
#         print(i)
#         print(x)
#         print()
#         print(training_count[i][x])
#         print(testing_count[j][y])
#         print(training_count[i][x] + testing_count[j][y])
#         print()
