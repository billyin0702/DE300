# Lab Writeup
All the code are in the [classify.py](ml/classify.py) file. In order to run the service, perform the following steps:

# Results
```
##################### Results #####################
Area Under ROC Curve: 0.8963
Selected Maximum Tree Depth: 6
Selected Number of Trees: 1000
##################################################
```

### Build Docker Image
```
chmod +x build-spark-image.sh
./build-spark-image.sh
```

### Run Docker Container
```
cd ml
chmod +x run.sh
./run.sh
```

### Run the service
When you are in the bash shell, run the following commands
```
cd ml
chmod +x run_py.sh
./run_py.sh
```