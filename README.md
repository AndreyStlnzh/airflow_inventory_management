## Project Structure
```
airflow-project/
├── dags/                       # Main DAG definitions
│   ├── analytical_dag.py       # Primary pipeline
│   └── catalogs_dag            # updating catalog data
├── etl/                        # Core business logic
│   
├── plugins/
│   ├── io/                     # Custom operators/hooks   
│   │   └── serializers         # dataframe ⇄ bytes      
```