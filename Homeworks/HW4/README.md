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
All contents will be on Amazon MWAA, the final DAG is `path_all_final.py` inside `billyin-de300-env`. The DAG will be run on a schedule, and the final output will be stored in the S3 bucket `de300spring2024-billyin`.

# Imputation Logic
The imputation logic is the same as HW1, Hw2, and HW3. However, `Imputers` are not used in this homework in the pipeline as it is faulty. Instead, they are simply filled with mode or mean on the spot

# Result
According the result in the photos, it is clear that the `merged smoke` linear regression is the best model with an accuracy of 0.897.

![res1.png](res1.png)
![res2.png](res2.png)