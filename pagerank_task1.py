from pyspark import SparkContext, SparkConf
import sys
import operator

# Function to calculate rank contributions
def calculate_rank(neighbors, rank):
    """Calculates the rank contribution for each neighbor."""
    num_neighbors = len(neighbors)
    for neighbor in neighbors:
        yield (neighbor, rank / num_neighbors)

# Function to validate input rows
def validate_rows(line):
    """Validates input lines: skips comments, empty lines, and invalid formats."""
    if line.startswith("#") or len(line.strip()) == 0:
        return False
    parts = line.strip().split("\t")
    return len(parts) == 2

# Function to parse rows into key-value pairs
def parse_rows(line):
    """Converts a line to lowercase and splits into key-value pairs."""
    items = line.lower().strip().split("\t")
    return items[0], items[1]

def main():
    # Initialize Spark Context
    conf = SparkConf().setAppName("PageRank-Task1").setMaster("spark://master:7077")
    sc = SparkContext(conf=conf)

    # Read input arguments
    input_path = sys.argv[1]    # e.g., hdfs://nn:9000/input/pagerank/web-BerkStan.txt
    output_path = sys.argv[2]   # e.g., hdfs://nn:9000/output/pagerank

    lines = sc.textFile(input_path)
    filtered_lines = lines.filter(validate_rows)
    edges = filtered_lines.map(parse_rows)
    
    links = edges.distinct().groupByKey()

    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Run PageRank for 10 iterations
    for iteration in range(10):
        # Calculate contributions using the provided logic
        contributions = links.join(ranks).flatMap(
            lambda x: calculate_rank(list(x[1][0]), x[1][1])
        )

        # Update ranks based on contributions
        ranks = contributions.reduceByKey(operator.add).mapValues(lambda rank: 0.15 + 0.85 * rank)

    # Save the final ranks to HDFS
    ranks.saveAsTextFile(output_path)

    # Stop Spark Context
    sc.stop()

if __name__ == "__main__":
    main()


