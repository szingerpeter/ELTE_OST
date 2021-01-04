import numpy as np
import rrcf
import math
import glob
import csv
from datetime import datetime

# Generate data
dataset_path = '/Users/liyang/Documents/Github/OST-SM/ELTE_OST/data_ingestion/data/2018_electric_power_data/adapt'
data_path = glob.glob(dataset_path + '/*.csv')


def csv_to_json(csvFilePath):
    jsonArray = []

    # read csv file
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        # convert each csv row into python dict
        for row in csvReader:
            jsonArray.append(row)

    return jsonArray


# Set tree parameters
num_trees = 30
shingle_size = 1
tree_size = 192


# Create a forest of empty trees
def create_forest(num_trees):
    forest = []
    for _ in range(num_trees):
        tree = rrcf.RCTree()
        forest.append(tree)

    return forest


forests = []
for _ in range(3):
    forests.append(create_forest(num_trees))


def decision_tree(forest, tree_size, index, point):
    avg_codisp = 0
    for tree in forest:
        # If tree is above permitted size...
        if len(tree.leaves) > tree_size:
            # Drop the oldest point (FIFO)
            tree.forget_point(index - tree_size)
        # Insert the new point into the tree
        tree.insert_point(point, index=index)
        # Compute codisp on the new point...
        new_codisp = tree.codisp(index)
        # And take the average over all trees
        avg_codisp += new_codisp / num_trees

    return avg_codisp


# outlier detection parameters
outlier_threshold = 3  # how many times away from standard deviation
sum_squared_value = [0, 0, 0]
sum_value = [0, 0, 0]
mean_d = [0, 0, 0]
var_d = [0, 0, 0]
std_d = [0, 0, 0]

# For each shingle...
count = 0
# for index, point in enumerate(points):
for index, path in enumerate(data_path):
    count += 1

    datas = csv_to_json(path)
    time_stamp = datetime.fromtimestamp(int(float(datas[0]['timestamp'])))
    print('---%s---' % time_stamp)

    locations = [[], [], []]
    for d in datas:
        locations[int(d['location_id'])].append(float(d['measurements']))
        # sin += point

    for ind, point in enumerate(locations):
        point = np.array(point)

        # forest = create_forest(num_trees)
        avg_codisp = decision_tree(forests[ind], tree_size, index, point)

        # detect outliers
        sum_value[ind] += avg_codisp
        sum_squared_value[ind] += avg_codisp ** 2
        mean_d[ind] = sum_value[ind] * 1.0 / count
        if count > 1:
            var_d[ind] = (sum_squared_value[ind] - mean_d[ind] ** 2 * count) / (count - 1)
            std_d[ind] = math.sqrt(var_d[ind])

            if std_d[ind] != 0:
                z_score = (avg_codisp - mean_d[ind]) / std_d[ind]
                if np.abs(z_score) > outlier_threshold:
                    print('Anomaly detected at location %d: ' % (ind), point)
