docker run -v "$(pwd)":/tmp/wc-demo  -it \
           -p 8888:8888 \
           --name wc-container \
	    py_spark_de300