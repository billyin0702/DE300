# Install docker
The services in this assignmnet requires docker, so they will need to be installed. They can be installed using the following commands...
```
sudo apt-get update
sudo apt install docker.io
sudo chmod 666 /var/run/docker.sock 
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