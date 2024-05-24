docker run -v "$(pwd)":/tmp/main_folder -it \
           -p 8888:8888 \
           --name de300-hw3-cont \
	    hw3_spark_image
