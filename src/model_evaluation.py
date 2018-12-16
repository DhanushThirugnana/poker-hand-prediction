import sys
import warnings

from sklearn import svm
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, BaggingClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression, Perceptron
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from sklearn.model_selection import cross_val_score
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.tree import DecisionTreeClassifier

from preprocessor import *

warnings.filterwarnings("ignore")

print_msg = ["DecisionTreeClassifier: criterion=entropy",
             "DecisionTreeClassifier : criterion=gini",
             "Perceptron : penalty='l1', n_iter=100",
             "Perceptron : penalty='l2', n_iter=100",
             "svm.LinearSVC : penalty='l1'",
             "svm.LinearSVC : penalty='l2'",
             "GaussianNB",
             "BaggingClassifier : n_estimators=5",
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
             "GradientBoostingClassifier : n_estimators=50, learning_rate=1",
             "GradientBoostingClassifier : n_estimators=50, learning_rate=0.1",
             "GradientBoostingClassifier : n_estimators=50, learning_rate=0.01",
             "GradientBoostingClassifier : n_estimators=100, learning_rate=1",
             "GradientBoostingClassifier : n_estimators=100, learning_rate=0.1",
             "GradientBoostingClassifier : n_estimators=100, learning_rate=0.01",
             "GradientBoostingClassifier : n_estimators=150, learning_rate=1",
             "GradientBoostingClassifier : n_estimators=150, learning_rate=0.1",
             "GradientBoostingClassifier : n_estimators=150, learning_rate=0.01",
             "MLPClassifier : solver='sgd', alpha=1e-5, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='sgd', alpha=1e-4, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='sgd', alpha=1e-3, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='sgd', alpha=1e-2, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='sgd', alpha=1e-1, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='adm', alpha=1e-5, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='adm', alpha=1e-4, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='adm', alpha=1e-3, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='adm', alpha=1e-2, hidden_layer_sizes= : 100, 70",
             "MLPClassifier : solver='adm', alpha=1e-1, hidden_layer_sizes= : 100, 70",
             "classifiers.append : LogisticRegression : penalty='l1', solver='liblinear'",
             "classifiers.append : LogisticRegression : penalty='l2', solver='newton-cg'",
             "classifiers.append : LogisticRegression : penalty='l2', solver='lbfgs'",
             "classifiers.append : LogisticRegression : penalty='l2', solver='liblinear'",
             "classifiers.append : LogisticRegression : penalty='l2', solver='sag'",
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
        DecisionTreeClassifier(criterion="entropy"),
        DecisionTreeClassifier(criterion="gini"),
        Perceptron(penalty='l1', n_iter=100),
        Perceptron(penalty='l2', n_iter=100),
        svm.LinearSVC(penalty='l1', dual=False),
        svm.LinearSVC(penalty='l2'),
        GaussianNB(),
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
        AdaBoostClassifier(n_estimators=50, learning_rate=0.1),
        GradientBoostingClassifier(n_estimators=50, learning_rate=1),
        GradientBoostingClassifier(n_estimators=50, learning_rate=0.1),
        GradientBoostingClassifier(n_estimators=50, learning_rate=0.01),
        GradientBoostingClassifier(n_estimators=100, learning_rate=1),
        GradientBoostingClassifier(n_estimators=100, learning_rate=0.1),
        GradientBoostingClassifier(n_estimators=100, learning_rate=0.01),
        GradientBoostingClassifier(n_estimators=150, learning_rate=1),
        GradientBoostingClassifier(n_estimators=150, learning_rate=0.1),
        GradientBoostingClassifier(n_estimators=150, learning_rate=0.01),
    ]
    for i in [1e-5, 1e-4, 1e-3, 1e-2, 1e-1]:
        classifiers.append(
            MLPClassifier(solver='sgd', alpha=i, hidden_layer_sizes=(100, 70), random_state=1, momentum=0.95,
                          activation="relu"))
        classifiers.append(
            MLPClassifier(solver='adam', alpha=i, hidden_layer_sizes=(100, 70), random_state=1, activation="relu"))
    classifiers.append(LogisticRegression(penalty='l1', solver='liblinear'))

    for i in ['newton-cg', 'lbfgs', 'liblinear', 'sag']:
        classifiers.append(LogisticRegression(penalty='l2', solver=i))

    for i in ['auto', 'ball_tree', 'kd_tree', 'brute']:
        classifiers.append(KNeighborsClassifier(n_neighbors=1, algorithm=i))
        classifiers.append(KNeighborsClassifier(n_neighbors=5, algorithm=i))
        classifiers.append(KNeighborsClassifier(n_neighbors=10, algorithm=i))

    return classifiers


if __name__ == '__main__':
    train_path, test_path = sys.argv[1:3]
    train_data, test_data = get_preprocessed_data(train_path, test_path)
    num_folds = int(sys.argv[3])
    models = get_classifiers()
    features = train_data.columns.values[:-1]
    cls = train_data.columns.values[-1]
    X = train_data[features].values
    y = train_data[cls].values

    '''print("yes")
    kf = KFold(n_splits=num_folds)
    kf.get_n_splits(X)  # returns the number of splitting iterations in the cross-validation
    print(kf)
    for train_index, test_index in kf.split(X):
        print('TRAIN:' + str(train_index) + 'TEST:' + str(test_index))
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        print(X_train)
        print(X_test)
        print(y_train)
        print(y_test)'''
    print_index = 0
    for c in range(len(models)):
        classifier = models[c].fit(X, y)
        scores = cross_val_score(classifier, X, y, cv=num_folds)
        print(print_msg[print_index], ",", np.average(scores))
        print_index += 1

    # Best Classifiers
    train_data, test_data = get_preprocessed_data('poker-train.csv', 'poker-test.csv')
    features = test_data.columns.values[:-1]
    cls = test_data.columns.values[-1]
    X = test_data[features].values
    y = test_data[cls].values

    classifier_index = [0, 3, 5, 6, 9, 12, 15, 29, 36, 38, 42, 47]
    print("Method,Cross Validation Score", "Accuracy Score", "Precision", "Recall", "fbeta_score", "support")
    for i in range(len(classifier_index)):
        c = models[classifier_index[i]].fit(X, y)
        scores = cross_val_score(c, X, y, cv=5)
        predictions = c.predict(X)
        print(print_msg[classifier_index[i]], ",",
              np.average(scores), ",",
              accuracy_score(y, predictions), ",",
              precision_recall_fscore_support(y, predictions)[0][0], ",",
              precision_recall_fscore_support(y, predictions)[1][0], ",",
              precision_recall_fscore_support(y, predictions)[2][0], ",",
              precision_recall_fscore_support(y, predictions)[3][0])
