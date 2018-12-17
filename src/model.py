import csv
import warnings

from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, BaggingClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import KFold
from sklearn.neighbors import KNeighborsClassifier

warnings.filterwarnings("ignore")
strout = ""

print_msg = ["BaggingClassifier : n_estimators=5",
             "BaggingClassifier : n_estimators=10",
             "BaggingClassifier : n_estimators=15",
             "BaggingClassifier : n_estimators=25",
             "RandomForestClassifier : max_depth=None, random_state=0",
             "RandomForestClassifier : max_depth=5, random_state=0",
             "RandomForestClassifier : max_depth=10, random_state=0",
             "AdaBoostClassifier : n_estimators=10, learning_rate=10",
             "AdaBoostClassifier : n_estimators=10, learning_rate=1",
             "AdaBoostClassifier : n_estimators=10, learning_rate=0.1",
             "AdaBoostClassifier : n_estimators=25, learning_rate=10",
             "AdaBoostClassifier : n_estimators=25, learning_rate=1",
             "AdaBoostClassifier : n_estimators=25, learning_rate=0.1",
             "AdaBoostClassifier : n_estimators=50, learning_rate=10",
             "AdaBoostClassifier : n_estimators=50, learning_rate=1",
             "AdaBoostClassifier : n_estimators=50, learning_rate=0.1",
             "KNeighborsClassifier : n_neighbors=1, algorithm='auto'",
             "KNeighborsClassifier : n_neighbors=1, algorithm='ball_tree'",
             "KNeighborsClassifier : n_neighbors=1, algorithm='kd_tree'",
             "KNeighborsClassifier : n_neighbors=1, algorithm='brute'",
             "KNeighborsClassifier : n_neighbors=5, algorithm='auto'",
             "KNeighborsClassifier : n_neighbors=5, algorithm='ball_tree'",
             "KNeighborsClassifier : n_neighbors=5, algorithm='kd_tree'",
             "KNeighborsClassifier : n_neighbors=5, algorithm='brute'",
             "KNeighborsClassifier : n_neighbors=10, algorithm='auto'",
             "KNeighborsClassifier : n_neighbors=10, algorithm='ball_tree'",
             "KNeighborsClassifier : n_neighbors=10, algorithm='kd_tree'",
             "KNeighborsClassifier : n_neighbors=10, algorithm='brute'"
             ]


def get_classifiers():
    classifiers = [
        BaggingClassifier(n_estimators=5),
        BaggingClassifier(n_estimators=10),
        BaggingClassifier(n_estimators=15),
        BaggingClassifier(n_estimators=25),
        RandomForestClassifier(max_depth=None, random_state=0),
        RandomForestClassifier(max_depth=5, random_state=0),
        RandomForestClassifier(max_depth=10, random_state=0),
        AdaBoostClassifier(n_estimators=10, learning_rate=10),
        AdaBoostClassifier(n_estimators=10, learning_rate=1),
        AdaBoostClassifier(n_estimators=10, learning_rate=0.1),
        AdaBoostClassifier(n_estimators=25, learning_rate=10),
        AdaBoostClassifier(n_estimators=25, learning_rate=1),
        AdaBoostClassifier(n_estimators=25, learning_rate=0.1),
        AdaBoostClassifier(n_estimators=50, learning_rate=10),
        AdaBoostClassifier(n_estimators=50, learning_rate=1),
        AdaBoostClassifier(n_estimators=50, learning_rate=0.1)
    ]

    for i in ['auto', 'ball_tree', 'kd_tree', 'brute']:
        classifiers.append(KNeighborsClassifier(n_neighbors=1, algorithm=i))
        classifiers.append(KNeighborsClassifier(n_neighbors=5, algorithm=i))
        classifiers.append(KNeighborsClassifier(n_neighbors=10, algorithm=i))

    return classifiers


"""if __name__ == '__main__':
    train_path, test_path = sys.argv[1:3]
    train_data, test_data = get_preprocessed_data(train_path, test_path)
    num_folds = int(sys.argv[3])
    models = get_classifiers()
    features = train_data.columns.values[:-1]
    cls = train_data.columns.values[-1]
    X = train_data[features].values
    y = train_data[cls].values
    """


def func(j):
    out = []
    j = j.take(j.count())
    for taken in j:
        out.append(taken)
    print(out)

    with open("DSTREAM.csv", "w+") as my_csv:
        csvWriter = csv.writer(my_csv, delimiter=',')
        csvWriter.writerows(out)


def kfolds(rdd_list):
    # rdd=rdd_list.flatMap(lambda x:[y for y in x])
    global strout
    ll = []
    for i in rdd_list:
        i.foreachRDD(func)
    # Reading from the CSV File


def ensem():
    data = pd.read_csv("DSTREAM.csv", sep=",", header=None)
    testdata = pd.read_csv("poker-hand-testing.data", sep=",", header=None)
    data.dropna(how="all", inplace=True)
    testdata.dropna(how="all", inplace=True)
    data = np.array(data)
    testdata = np.array(testdata)
    X = data[:, :-1]
    y = data[:, 10]

    X_test = testdata[:, :-1]
    y_test = testdata[:, 10]
    # print(testdata.shape, X.shape, y.shape)
    models = get_classifiers()
    print_index = 0
    kf = KFold(n_splits=9)
    accudict = {}
    indexdict = {}

    for train_index, val_index in kf.split(X):
        X_train, X_val = X[train_index], X[val_index]
        y_train, y_val = y[train_index], y[val_index]
        print_index = 0
        accur = []

        for c in range(len(models)):
            classifier = models[c].fit(X_train, y_train)
            # scores = model.cross_val_scorse(classifier, X_val, y_val, cv=9)
            predictions = classifier.predict(X_val)
            # print(model.print_msg[print_index], ",", accuracy_score(y_val, predictions))
            scr = accuracy_score(y_val, predictions)
            keystr = str(print_msg[print_index]).split(" : ")[0]
            # print(keystr)
            if keystr not in accudict.keys():
                accudict[keystr] = -10
                indexdict[keystr] = []
            if keystr in accudict.keys():
                if accudict[keystr] < scr:
                    accudict[keystr] = scr
                    indexdict[keystr] = train_index
            print_index += 1
    # for i in accudict.items():
    #     print(i)
    # for j in indexdict.items():
    #     print(j)
    output = {i: j for i in indexdict.keys() for j in zip(indexdict.values(), accudict.values())}
    for i in output:
        print(i)
