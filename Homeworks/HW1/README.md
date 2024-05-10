# General
Homework is stored in hw1.ipynb, the script to insert the data is in the first few blocks

# Install docker
The services in this assignmnet requires docker, so they will need to be installed. They can be installed using the following commands...
```
sudo apt-get update
sudo apt install docker.io
```

# Socket Issue
If anything related to docker.sock and says permission denied, try running the following...
```
sudo chmod 666 /var/run/docker.sock 
```

# Building images
If you have not built the images, you can do so by running the following commands...
```
docker build -t my_jupyter .
```

# Start up the services
When starting up the services, please run the following commands
```
chmod +x start_network.sh
chmod +x start_mysql.sh
chmod +x start_code.sh

./start_network.sh
./start_mysql.sh
```

When all is done, please start the code docker container...

```
./start_code.sh
```

This will start the service here: http://127.0.0.1:8888/lab/workspaces/auto-e. You can remove the -d in the ./start_code.sh if you would like to run in terminal.

# Starting on EC2
You can modify ./start_code.sh to be the following if you have a path like mine to make it absolute...
```
docker run --network de300_net \
    --name code \
    -p 8888:8888 \
    -v /01-DE300/DE300/Homeworks/HW1:/home/jovyan/ my_jupyter
```