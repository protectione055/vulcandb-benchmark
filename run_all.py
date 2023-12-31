import os
import configparser

from task3_cost_estimation.rawfile_task3_impl import RawfileTask3Impl
from task3_cost_estimation.postgresql_task3_impl import PGTask3Impl

# 读取配置文件
config = configparser.ConfigParser()
config.read("config.ini")

class TaskFactory:
    def get_task3(self, test_class):
        if test_class == "RAWFILE":
            return RawfileTask3Impl()
        elif test_class == "POSTGRESQL":
            user = config.get('POSTGRESQL', 'user')
            password = config.get('POSTGRESQL', 'password')
            host = config.get('POSTGRESQL', 'host')
            port = config.get('POSTGRESQL', 'port')
            args = {"user": user, "password": password, "host": host, "port": port}
            return PGTask3Impl(args)
        else:
            raise Exception("No such test class: " + test_class)

if "__main__" == __name__:
    task_facotry = TaskFactory()
    pwd = os.getcwd()
    test_class = config.get('COMMON', 'test')
    
    # Task3 Startup
    task3_workloads = ["datasets/task3/workload1.ifc"]
    for i in range(len(task3_workloads)):
        task3_workloads[i] = os.path.join(pwd, task3_workloads[i])
    task3 = task_facotry.get_task3(test_class)
    # task3.prepare_data(task3_workloads)
    ground_true = task_facotry.get_task3("RAWFILE")
    ground_true.prepare_data(task3_workloads)
    
    # Task3 Job1
    result_job1 = task3.run_job1()
    ground_true_job1 = ground_true.run_job1()
    print(f"Task3 Job1: {result_job1}")
    print(f"Ground true Job1: {ground_true_job1}")
    
    # Task3 Cleanup
    task3.cleanup()
    ground_true.cleanup()