from airflow.sdk import dag, task


@dag
def xcoms_auto_dag():
    @task.python
    def first_task():
        print("Extracting data...")
        data = {"keys": ["2", "3", "4", "5", "1", "0"]}
        return data

    @task.python
    def second_task(data: dict):
        print("Transforming data...")
        transformed_data = sorted(data["keys"])
        transformed_data = transformed_data * 2
        return transformed_data
        
    @task.python
    def third_task(transformed_data: list):
        print("Loading data...")
        print(f"Final transformed data: {transformed_data}")
        

    first = first_task()
    second = second_task(first)
    third = third_task(second)

    
# Instantiate the DAG
xcoms_auto_dag()