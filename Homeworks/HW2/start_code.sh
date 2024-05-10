docker run --name code \
    -p 8888:8888 \
    -v "$(pwd)":/home/jovyan/ \
    -d my_jupyter \

# Install necessary packages
docker exec -it code pip install \
    scikit-learn boto3 pandas matplotlib scrapy