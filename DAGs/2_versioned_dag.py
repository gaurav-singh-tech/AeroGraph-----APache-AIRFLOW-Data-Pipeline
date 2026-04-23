from airflow.sdk import dag, task #It is important to import the task decorator from the sdk module, not from the main airflow module

@dag(
    dag_id ="versioned_dag",
)
def versioned_dag():#The dag decorator is used to define a DAG, and it takes the dag_id as an argument to uniquely identify the DAG
    @task.python #The task decorator is used to define a task within the DAG, and it specifies that the task is a Python function
    def first_task():
        print("This is the first task of the first DAG!")

    @task.python
    def second_task():
        print("This is the second task of the first DAG!")
        
    @task.python
    def third_task():   
        print("This is the third task of the first DAG!, DAG Complete  version 2.0!")
        
    #Defining the task dependencies using the bitshift operator (>>). This means that first_task will run before second_task, and second_task will run before third_task.

    first=first_task()
    second=second_task()
    third=third_task()
    first >> second >> third

#Registering the DAG    
versioned_dag() #Finally, we call the function to create an instance of the DAG/to register the DAG with Airflow.
