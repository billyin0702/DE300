from pyspark import SparkContext, SparkConf

DATA = "./data/*.txt"
OUTPUT_DIR = "counts" # name of the folder

def word_count():
    """
    Word count example using Spark RDD.
    """
    sc = SparkContext("local","Word count example")
    textFile = sc.textFile(DATA)

    # Flatmap the counts and also strip the word of any special characters
    counts = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word.strip(".,;:!?\"\'"), 1)).reduceByKey(lambda a, b: a + b)

    # Find the number of words in the text file that have counts greater than 3, then print them out
    filtered = counts.filter(lambda x: x[1] >= 3)
    filtered.saveAsTextFile(OUTPUT_DIR)

word_count()
