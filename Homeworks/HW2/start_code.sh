docker run --network de300_net \
    --name code \
    -p 8888:8888 \
    -v "$(pwd)":/home/jovyan/ \
    -d my_jupyter \

# Install necessary packages
docker exec -it code pip install \
    mysql-connector-python 