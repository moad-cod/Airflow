from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator


@dag
def operator_dag():
    @task.python
    def first_task():
        print("this is the first task")
        
    @task.python
    def second_task():
        print("this is the second task")
        
    @task.bash
    def bash_task_modern() -> str:
        return "echo https://airflow.apache.org/"
    
    bash_task_old = BashOperator(
        task_id="run_after_loop",
        bash_command="echo https://airflow.apache.org/",
    )
        
    first = first_task()
    second = second_task()
    bash_modern = bash_task_modern()
    bash_old = bash_task_old
    
    first >> second >> bash_modern >> bash_old

# Instantiate the DAG
operator_dag()
    


