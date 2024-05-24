# Homework 3
All write ups are inside the [EXPLAIN.md](EXPLAIN.md) file. The code is inside the [main_folder](main_folder) directory.

## 1. Running the service locally
```
chmod +x ./build_dockersh
./build_docker.sh

cd main_folder
chmod +x ./run_docker.sh
chmod +x rs1.sh
chmod +x rs2.sh
./run_docker.sh
```
Then, you should run the initialy cleaning script then the advanced cleaning + modeling script.
```
cd main_folder
./rs1.sh
./rs2.sh
```

## 2. Running the service on AWS EMR
When using EMR Linux 2, the versino of openSSL supported on the proposed 6.10 versions will not work as they will have openSSL version 1.1.0. For the requests package to work on Python, it has to be 1.1.1+ at least. So, use a higher version of EMR will help.