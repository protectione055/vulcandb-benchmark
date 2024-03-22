import os
import configparser

from task3_cost_estimation.rawfile_task3_impl import RawfileTask3Impl
from task3_cost_estimation.postgresql_task3_impl import PGTask3Impl
from task1_geom_edit.rawfile_task1_impl import RawfileTask1Impl

# 读取配置文件
config = configparser.ConfigParser()
config.read("config.ini")

class TaskFactory:
    def get_task1(self, test_class):
        if test_class == "RAWFILE":
            return RawfileTask1Impl()
        else:
            raise Exception("No such test class: " + test_class)
        
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
    
    if config.get('COMMON', 'run_task1') == "True":
        # Task1 Startup
        task1_test = task_facotry.get_task1(test_class)
        task1_test.prepare_data(None)
        task1_test.run()
        task1_test.cleanup()
    
    if config.get('COMMON', 'run_task3') == "True":
        # Task3 Startup
        task3_workloads = ["datasets/task3/workload1.ifc"]
        for i in range(len(task3_workloads)):
            task3_workloads[i] = os.path.join(pwd, task3_workloads[i])
        task3_test = task_facotry.get_task3(test_class)
        print("Task3 test class: " + test_class + " preparing data...")
        task3_test.prepare_data(task3_workloads)
        print("Task3 ground true preparing data...")
        task3_ground_true = task_facotry.get_task3("RAWFILE")
        task3_ground_true.prepare_data(task3_workloads)
        
        # Task3 Job1
        print("Task3 test class: " + test_class + " running...")
        task3_result = task3_test.run()
        print("Task3 ground true running...")
        task3_expected = task3_ground_true.run()
        if task3_result == task3_expected:
            print("Task3 all query passed.")
        else:
            print("Task3 query failed.")
            print("Expected:")
            print(task3_expected)
            print("Actual:")
            print(task3_result)
        
        # Task3 Cleanup
        # task3_test.cleanup()
        task3_ground_true.cleanup()