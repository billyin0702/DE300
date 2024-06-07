# Some Notes
Path 1 - Using normal pandas to read the data
Path 2 - Using PySpark to read the data
Merge - Path 2 but smoke is not imputed

# File Structure on S3
```
de300spring2024
    ├── heart_disease.csv
    ├── config.toml
    ├── dags
    │   └── path_all_final.py
    ├── requirements.txt

```

# Running the DAG
All contents will be on Amazon MWAA, the final DAG is `path_all_final.py`. The DAG will be run on a schedule, and the final output will be stored in the S3 bucket `de300spring2024`.