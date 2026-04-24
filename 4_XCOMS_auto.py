from airflow.sdk import dag, task #It is important to import the task decorator from the sdk module, not from the main airflow module
from airflow.decorators import dag, task

@dag(
    dag_id ="XCOMS_dag_auto",
)
def XCOMS_dag_auto():#The dag decorator is used to define a DAG, and it takes the dag_id as an argument to uniquely identify the DAG
    @task.python #The task decorator is used to define a task within the DAG, and it specifies that the task is a Python function
    def first_task():
        print("Extracting the data from the source...This is the first task of the first DAG!")
        fetched_data={"data":[1,2,3,4,5]} #This is a dummy data that we are fetching from the source
        return fetched_data

    @task.python
    def second_task(data: dict):
        print("Transforming the data...This is the second task of the first DAG!")
        fetched_data=data["data"] #This is how we can access the data that we fetched in the first task
        transformed_data=fetched_data*2 #This is a dummy transformation that we are doing on the data
        transformed_data_dict={"transformed_data":transformed_data} #We are converting the transformed data into a dictionary so that we can pass it to the next task
        return transformed_data_dict

    @task.python
    def third_task(data: dict):   
        print("Loading the data...This is the third task of the first DAG!, DAG Complete")
        load_data=data["transformed_data"] #This is how we can access the transformed data that we got from the second task
        return load_data
  
        
    #Defining the task dependencies using the bitshift operator (>>). This means that first_task will run before second_task, and second_task will run before third_task.

    first=first_task()
    second=second_task(first) #We are passing the output of the first task as an argument to the second task
    third=third_task(second) #We are passing the output of the second task as an argument to the third task
    
    #first >> second >> third, Now you dontt need to define the dependensies using the bitshift operator (>>), because we are already passing the output of one task to the next task, so Airflow can automatically infer the dependencies between the tasks.

#Registering the DAG    
XCOMS_dag_auto() #Finally, we call the function to create an instance of the DAG/to register the DAG with Airflow.
