docker run -v "$(pwd)":/tmp/ml -it \
           -p 8888:8888 \
           --name pyspark-lab07-container \
	    pyspark-lab07-image
