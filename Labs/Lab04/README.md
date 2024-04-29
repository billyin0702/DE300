# How to Setup Lab Environment
Please have an image built already called ```my_jupyter```, the instructions to do so are listed [Here](https://github.com/suzhesuzhe/DE300/blob/main/lab_doc/lab2.ipynb).

# Start up the docker containers (similar to HW1)
Please follow the ordering of the sections to setup properly.

## Install docker
The services in this assignmnet requires docker, so they will need to be installed. They can be installed using the following commands...
```
sudo apt-get update
sudo apt install docker.io
```

## Socket Issues
If anything related to docker.sock and says permission denied, try running the following...
```
sudo chmod 666 /var/run/docker.sock 
```

**Note:** This above command may only work on linux

## Start up the services
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