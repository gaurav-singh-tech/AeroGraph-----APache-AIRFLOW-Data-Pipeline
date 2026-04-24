from airflow.decorators import dag, task  # fixed import
from airflow.providers.standard.operators.bash import BashOperator  # fixed import

@dag(
    dag_id="bashoperator_dag",
)
def bashoperator_dag():  #The dag decorator is used to define a DAG, and it takes the dag_id as an argument to uniquely identify the DAG
    
    @task  # simplified (no need for .python)
    def first_task():
        print("This is the first task of the first DAG!")

    @task
    def second_task():
        print("This is the second task of the first DAG!")

    #BASH OPERATOR
    @task.bash
    def bash_task_modern() -> str:
        return "echo https://airflow.apache.org/"

    bash_task_oldschool = BashOperator(
        task_id="bash_task_oldschool",
        bash_command="echo https://airflow.apache.org/",
    )
    
    #Defining dependencies
    first = first_task()
    second = second_task()
    bash_modern = bash_task_modern()
    bash_oldschool = bash_task_oldschool

    first >> second >> bash_modern >> bash_oldschool

#Registering the DAG    
bashoperator_dag()


#COOMENTS to understand entire code:

# from airflow.decorators import dag, task  
# # Importing 'dag' and 'task' decorators
# # dag → used to define a workflow
# # task → used to define tasks inside the workflow

# from airflow.providers.standard.operators.bash import BashOperator  
# # Importing BashOperator
# # This lets us run terminal (bash) commands as a task in Airflow


# @dag(
#     dag_id="bashoperator_dag",
# )
# # @dag decorator defines a DAG (workflow)
# # dag_id is the unique name of this workflow in Airflow UI

# def bashoperator_dag():  
#     # This function contains all tasks and their execution order

    
#     @task  
#     # This converts a normal Python function into an Airflow task
#     def first_task():
#         print("This is the first task of the first DAG!")
#         # This will print the message in logs when task runs


#     @task
#     def second_task():
#         print("This is the second task of the first DAG!")
#         # Another simple Python task


#     # ------------------ BASH TASK (MODERN WAY) ------------------ #
#     @task.bash  
#     # This decorator creates a bash task (runs terminal commands)
#     def bash_task_modern() -> str:
#         # Function must return a string (bash command)
#         return "echo https://airflow.apache.org/"
#         # 'echo' prints the text in terminal/logs
#         # This does NOT open the website, just prints it


#     # ------------------ BASH TASK (OLD WAY) ------------------ #
#     bash_task_oldschool = BashOperator(
#         task_id="bash_task_oldschool",
#         # Unique name for this task

#         bash_command="echo https://airflow.apache.org/",
#         # Bash command that will run in terminal
#         # Again, it only prints the link, not opens it
#     )


#     # ------------------ EXECUTION FLOW ------------------ #

#     first = first_task()  
#     # Calling the function → creates task instance

#     second = second_task()  
#     # Same here

#     bash_modern = bash_task_modern()  
#     # Calling modern bash task (function-based)

#     bash_oldschool = bash_task_oldschool  
#     # This is already an object → DO NOT use ()
#     # Operators are not called like functions


#     # ------------------ TASK DEPENDENCIES ------------------ #
#     first >> second >> bash_modern >> bash_oldschool  
#     # This defines execution order:
#     # first_task → second_task → bash_task_modern → bash_task_oldschool


# # ------------------ DAG REGISTRATION ------------------ #
# bashoperator_dag()  
# # This line registers the DAG with Airflow
# # Without this, Airflow will not detect your DAG