from airflow.sdk import dag, task


@dag
def branch_dag():
    @task.python
    def extract_task(**kwargs):
        print("Extracting data .....")
        ti = kwargs['ti']
        extracted_data_dict = {
            "api_data": [1, 2, 3, 4, 5], 
            "db_data": [6, 7, 8, 9, 10], 
            "s3_data": [11, 12, 13, 14, 15],
            "weekend_flag": "false"
        }
        ti.xcom_push(key="return_value", value=extracted_data_dict)
        
    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(key="return_value", task_ids="extract_task")["api_data"]
        print(f"Transforming api data: {api_extracted_data} .....")
        transformed_api_data = [x * 2 for x in api_extracted_data]
        print(f"Transformed api data: {transformed_api_data}")
        ti.xcom_push(key="return_value", value=transformed_api_data)
        
    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(key="return_value", task_ids="extract_task")["db_data"]
        print(f"Transforming db data: {db_extracted_data} .....")
        transformed_db_data = [x * 2 for x in db_extracted_data]
        print(f"Transformed db data: {transformed_db_data}")
        ti.xcom_push(key="return_value", value=transformed_db_data)
        
    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(key="return_value", task_ids="extract_task")["s3_data"]
        print(f"Transforming s3 data: {s3_extracted_data} .....")
        transformed_s3_data = [x * 2 for x in s3_extracted_data]
        print(f"Transformed s3 data: {transformed_s3_data}")
        ti.xcom_push(key="return_value", value=transformed_s3_data)
        
        
    # Decider branch
    @task.branch
    def decide_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(key="return_value", task_ids="extract_task")["weekend_flag"]
        if weekend_flag == "true":
            return "no_load_task"
        else:
            return "load_task"
    
    @task.python
    def load_task(**kwargs):
        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids="transform_task_api")
        db_data = ti.xcom_pull(task_ids="transform_task_db")
        s3_data = ti.xcom_pull(task_ids="transform_task_s3")
        print(f"Loading transformed data: {api_data}, {db_data}, {s3_data} .....")
    
    @task.python
    def no_load_task(**kwargs):
        print("No loading task executed due to weekend flag being true.")
            
    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    decide = decide_task()
    load = load_task()
    no_load = no_load_task()
    
    extract >> [transform_api, transform_db, transform_s3] >> decide >> [load, no_load]

# Instantiate the DAG
branch_dag()