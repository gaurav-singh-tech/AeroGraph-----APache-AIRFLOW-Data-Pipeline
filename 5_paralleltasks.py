from airflow.sdk import dag, task #It is important to import the task decorator from the sdk module, not from the main airflow module

@dag(
    dag_id ="parallel_dag",
)
def parallel_dag():#The dag decorator is used to define a DAG, and it takes the dag_id as an argument to uniquely identify the DAG
    @task.python #The task decorator is used to define a task within the DAG, and it specifies that the task is a Python function
    def extract_task(**kwargs):
        print("Extracting the data from the source...This is the first task of the parallel DAG!")
        ti=kwargs["ti"] #We are using the task instance (ti) to push the data to XCOMS, task instance in very simple language means the current task that is running, so we can use the task instance to push the data to XCOMS and then we can pull the data from XCOMS in the next task.
        extracted_data={"api_extracted_data": [1, 2, 3, 4, 5],
                        "db_extracted_data": [6, 7, 8, 9, 10],
                        "s3_extracted_data": [11, 12, 13, 14, 15]} #This is a dummy data that we are fetching from the source
        ti.xcom_push(key="extracted_data", value=extracted_data) #Here we are pushing the extracted data to XCOMS using the task instance (ti) and we are giving a key to the data so that we can pull the data using the same key in the next task.

    @task.python
    def transform_task_api(**kwargs):
        print("Transforming the data...This is the second task of the parallel DAG!")
        ti=kwargs["ti"]
        api_extracted_data=ti.xcom_pull(task_ids="extract_task", key="extracted_data")["api_extracted_data"] #Here we are pulling the extracted data from XCOMS using the task instance (ti) and we are using the same key that we used to push the data to XCOMS.
        transform_api_data=[i*2 for i in api_extracted_data] #This is a dummy transformation that we are doing on the data
        ti.xcom_push(key="transform_api_data", value=transform_api_data) #Here we are pushing the transformed data to XCOMS using the task instance (ti) and we are giving a key to the data so that we can pull the data using the same key in the next task.

    @task.python
    def third_task(**kwargs):
        print("Loading the data...This is the third task of the parallel DAG!, DAG Complete")
        ti=kwargs["ti"]
        transform_api_data=ti.xcom_pull(task_ids="transform_task_api", key="transform_api_data") #Here we are pulling the transformed data from XCOMS using the task instance (ti) and we are using the same key that we used to push the data to XCOMS.
        print(f"Transformed API Data: {transform_api_data}") #Here we are just printing the transformed data, but in real scenario we can load the data to the destination.
    
    @task.python
    def transform_task_db(**kwargs):
        print("Transforming the data...This is the fourth task of the parallel DAG!")
        ti=kwargs["ti"]
        db_extracted_data=ti.xcom_pull(task_ids="extract_task", key="extracted_data")["db_extracted_data"] #Here we are pulling the extracted data from XCOMS using the task instance (ti) and we are using the same key that we used to push the data to XCOMS.
        transform_db_data=[i*3 for i in db_extracted_data] #This is a dummy transformation that we are doing on the data
        ti.xcom_push(key="transform_db_data", value=transform_db_data) #Here we are pushing the transformed data to XCOMS using the task instance (ti) and we are giving a key to the data so that we can pull the data using the same key in the next task.

    
    @task.python
    def load_task(**kwargs):
        print("Loading the data...This is the fifth task of the parallel DAG!, DAG Complete")
        ti=kwargs["ti"]
        api_transformed_data=ti.xcom_pull(task_ids="transform_task_api", key="transform_api_data") #Here we are pulling the transformed data from XCOMS using the task instance (ti) and we are using the same key that we used to push the data to XCOMS.
        db_transformed_data=ti.xcom_pull(task_ids="transform_task_db", key="transform_db_data") #Here we are pulling the transformed data from XCOMS using the task instance (ti) and we are using the same key that we used to push the data to XCOMS.
        print(f"Transformed API Data: {api_transformed_data}") #Here we are just printing the transformed data, but in real scenario we can load the data to the destination.
        print(f"Transformed DB Data: {db_transformed_data}") #Here we are just printing the transformed data, but in real scenario we can load the data to the destination.
        
        
        #Here we are just printing the transformed data, but in real scenario we can load the data to the destination.
    
    
    #Defining the task dependencies using the bitshift operator (>>). This means that first_task will run before second_task, and second_task will run before third_task.

    extract=extract_task()
    transform_api=transform_task_api()
    transform_db=transform_task_db()
    load=load_task()
    
    extract >> [transform_api, transform_db] >> load #Here we are defining the dependencies between the tasks, so extract task will run first and then transform_api and transform_db will run in parallel and then load task will run after both transform_api and transform_db are completed.


#Registering the DAG    
parallel_dag() #Finally, we call the function to create an instance of the DAG/to register the DAG with Airflow.
