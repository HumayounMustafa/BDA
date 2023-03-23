#!/usr/bin/env python
#Run this command in terminal python kmeans_hadoop.py to run it.
import sys
import random

# Set the number of clusters and iterations
k = 3
iterations = 10

# Set the Hadoop streaming path
streaming_path = "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.1.jar"

# Set the input and output paths
input_path = "/input/data.txt"
output_path = "/output"

# Set the initial centroids
centroids = []
with open(input_path, "r") as infile:
    lines = infile.readlines()
    for i in range(k):
        centroids.append(random.choice(lines).strip())

# Run the iterations
for i in range(iterations):
    # Run the MapReduce job
    command = "hadoop jar {} -mapper mapper.py -reducer reducer.py -input {} -output {} -file mapper.py -file reducer.py -file centroids.txt".format(streaming_path, input_path, output_path)
    os.system(command)

    # Update the centroids
    with open(output_path + "/part-00000", "r") as infile:
        lines = infile.readlines()
        new_centroids = []
        for j in range(k):
            data_points = []
            for line in lines:
                centroid_id, data_point = line.strip().split("\t")
                if centroid_id == str(j):
                    data_points.append(data_point)
            new_centroid = calculate_mean(data_points)
            new_centroids.append(new_centroid)
    centroids = new_centroids

# Define the function to calculate the mean
def calculate_mean(data_points):
    x_total = 0
    y_total = 0
    count = 0
    for data_point in data_points:
        x, y = data_point.split(",")
        x_total += float(x)
        y_total += float
