from airflow.sdk import dag, task


@dag
def versioning_dag():
    @task.python
    def first_task():
        print("this is the first task")
        
    @task.python
    def second_task():
        print("this is the second task")
        
    @task.python
    def third_task():
        print("this is the third task")
        
    @task.python
    def version_task():
        print("this is the version task")
        
    first = first_task()
    second = second_task()
    third = third_task()
    version = version_task()
     
    first >> second >> third >> version
    
# Instantiate the DAG
versioning_dag()