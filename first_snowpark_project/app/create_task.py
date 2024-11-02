from snowflake.core import Root
import snowflake.connector
from datetime import timedelta
from snowflake.core.task import Task, StoredProcedureCall
import procedures
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation,CreateMode

conn = snowflake.connector.connect()
print("connection successful..")
print(conn)

root=Root(conn)
print(root)

my_task = Task("my_task",StoredProcedureCall(procedures.hello_procedure,stage_location="@dev_deployment"),\
    warehouse="compute_wh",schedule=timedelta(hours=1))

tasks = root.databases["db_demo"].schemas["public"].tasks

#tasks.create(my_task)

#create DAG
with DAG("my_dag",schedule=timedelta(days=1)) as dag:
    dag_task_1 = DAGTask("my_hello_task",StoredProcedureCall(procedures.hello_procedure,args=["madhava"],\
        packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
        stage_location="@dev_deployment"),warehouse="compute_wh")

    dag_task_2 = DAGTask("my_test_task",StoredProcedureCall(procedures.test_procedure,\
        packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
        stage_location="@dev_deployment"),warehouse="compute_wh")
    
    dag_task_3=DAGTask("my_test_task_1",StoredProcedureCall(procedures.test_procedure_two,packages=["snowflake-snowpark-python"],\
        stage_location="@dev_deployment",imports=["@dev_deployment/my_snowpark_project/app.zip"]),\
        warehouse="compute_wh")

    dag_task_4=DAGTask("my_test_task_2",StoredProcedureCall(procedures.test_procedure_two,packages=["snowflake-snowpark-python"],\
        stage_location="@dev_deployment",imports=["@dev_deployment/my_snowpark_project/app.zip"]),\
        warehouse="compute_wh")

    dag_task_1 >> dag_task_2 >>[dag_task_3,dag_task_4]

    schema=root.databases["db_demo"].schemas["public"]
    dag_operation=DAGOperation(schema)
    dag_operation.deploy(dag,CreateMode.or_replace)




