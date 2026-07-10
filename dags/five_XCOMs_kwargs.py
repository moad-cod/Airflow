from airflow.sdk import dag, task


@dag
def xcoms_kwargs_dag():
    @task.python
    def first_task(**kwargs):
        # Extracting ti from kwargs to push XComs manually
        ti = kwargs["ti"]
        print("Extracting data...")
        data = {"keys": ["2", "3", "4", "5", "1", "0"]}
        ti.xcom_push(key="data", value=data)

    @task.python
    def second_task(**kwargs):
        # Extracting ti from kwargs to pull XComs manually
        
        ti = kwargs["ti"]
        print("Transforming data...")
        data = ti.xcom_pull(task_ids="first_task", key="data")
        transformed_data = sorted(data["keys"])
        transformed_data = transformed_data * 2
        ti.xcom_push(key="transformed_data", value=transformed_data)
        
    @task.python
    def third_task(**kwargs):
        # Extracting ti from kwargs to pull XComs manually
        ti = kwargs["ti"]
        print("Loading data...")
        transformed_data = ti.xcom_pull(task_ids="second_task", key="transformed_data")
        print(f"Final transformed data: {transformed_data}")
        

    first = first_task()
    second = second_task()
    third = third_task()
    
    first >> second >> third

    
# Instantiate the DAG
xcoms_kwargs_dag()