# Some Notes
Path 1 - Using normal pandas to read the data and then use RDS to store the data
Path 2 - Using PySpark to read the data and then use RDS to store the data
Merge - Path 2 but smoke is not imputed

# Dag Structure Notes
- Path 1 and Path 2 are independent of each other
- db_connection is not used here, all postgres operations will be wrapped around python functions

# File Structure on S3
```
de300spring2024
    ├── heart_disease.csv
    ├── config.toml
    ├── dags
    │   └── path1.py
    │   └── path2.py
    │   └── merge.py
    ├── smoke_info
    │   └── smoke.csv

```

# Merging Smoke
For merging the smoke data, it will be a separate file and must be run _before_ the paths files. They are not in the same path due to dependency issues.