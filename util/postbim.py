import os
import psycopg2
import ifcopenshell
import ifcopenshell.util.selector as selector
import ifcopenshell.util.element
from ifcopenshell.api import run
from ifcopenshell import express

from util.common import Timer
from task3_cost_estimation.task3 import CostEstimator
import util.common as util

class Postbim:
    def __init__(self, args):
        self.__database_name = "postgres"
        self.__user = args["user"]
        self.__password = args["password"]
        self.__host = args["host"]
        self.__port = args["port"]
        
        try:
            self.__conn = psycopg2.connect(database=self.__database_name, user=self.__user, password=self.__password, host=self.__host, port=self.__port)
        except Exception as e:
            print(e)
            raise Exception("Failed to connect to database.")
        
    def __get_create_table_sql(self, entity):
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {entity.is_a()} (id INTEGER PRIMARY KEY, "  # 创建表的SQL语句
        attr_type_map = {
            "INT": "INTEGER",
            "DOUBLE": "DOUBLE PRECISION",
            "BOOL": "BOOLEAN",
            "LOGICAL": "VARCHAR(10)",
            "STRING": "VARCHAR(150)",
            "ENUMERATION": "VARCHAR(150)",
            "ENTITY INSTANCE": "INTEGER",
            "AGGREGATE OF ENTITY INSTANCE": "INTEGER[]",
            "AGGREGATE OF DOUBLE": "DOUBLE PRECISION[]",
            "AGGREGATE OF INT": "INTEGER[]",
            "AGGREGATE OF STRING": "text[]",
        }
        
        for i in range(len(entity)):
            attr_name = entity.attribute_name(i)
            attr_type = entity.attribute_type(i)
            
            if entity.is_a("ifcpropertysinglevalue") and attr_name == "NominalValue":
                attr_type = "TEXT"
            elif(attr_type in attr_type_map):
                attr_type = attr_type_map[attr_type]
            elif attr_type == "DERIVED":
                continue
            else:
                raise Exception(f"Unknown attribute type: {attr_type}, when creating table for {attr_name} in {entity}. info: {entity.get_info()}")
            create_table_sql += f"\"{attr_name}\" {attr_type}," # 列名需要加引号，因为有些列名是SQL关键字(如: Outer)
        create_table_sql = create_table_sql[:-1] + ");"
        
        return create_table_sql
    
    def __get_insert_sql(self, entity):
        insert_sql = f"INSERT INTO {entity.is_a()} VALUES ({entity.id()},"
        attr_info = entity.get_info()
        
        for i in range(len(entity)):
            attr_name = entity.attribute_name(i)
            attr_type = entity.attribute_type(i)
            attr_value = attr_info[entity.attribute_name(i)]
            
            if entity.is_a("ifcpropertysinglevalue") and attr_name == "NominalValue":
                insert_sql += f"'{str(attr_value.get_info()['wrappedValue']).replace("'", "''")}',"
                continue
                    
            if attr_type == "DERIVED":
                continue
            if attr_value is None:
                insert_sql += "NULL,"
            elif attr_type == "DOUBLE" or attr_type == "INT":
                insert_sql += f"{attr_value},"
            elif attr_type == "STRING" or attr_type == "ENUMERATION" or attr_type == "LOGICAL":
                if isinstance(attr_value, str):
                    attr_value = attr_value.replace("'", "''")
                insert_sql += f"'{attr_value}',"
            elif attr_type == "BOOL":
                insert_sql += f"{attr_value},"
            elif attr_type == "ENTITY INSTANCE":
                # #123 -> 123
                attr_value = str(attr_value.id())
                insert_sql += f"{attr_value},"
            elif attr_type.startswith("AGGREGATE OF"):
                if attr_type == "AGGREGATE OF ENTITY INSTANCE":
                    # (#1,#2,#3) --> '{1,2,3}'
                    attr_value = [x.id() for x in attr_value]
                elif attr_type == "AGGREGATE OF STRING":
                    # ('a','b','c') --> '{"a","b","c"}'
                    attr_value = ['"' + x.replace('"', '""') + '"' for x in attr_value]
                attr_value = [str(x) for x in attr_value]
                insert_sql += f"""\'{{{",".join(attr_value)}}}',"""
        insert_sql = insert_sql[:-1] + ");"
        
        return insert_sql

    def __createdb_for_model(self, user, password, host, port, model_name):
            """
            创建数据库并返回连接对象。

            参数：
            user (str)：数据库用户名。
            password (str)：数据库密码。
            host (str)：数据库主机地址。
            port (int)：数据库端口号。
            model_name (str)：要创建的数据库名称。

            返回：
            conn (psycopg2.extensions.connection)：数据库连接对象。
            """
            try:
                conn = self.__conn
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(f"CREATE DATABASE {model_name}")
            except psycopg2.errors.DuplicateDatabase:
                pass
            except Exception as e:
                raise e
            finally:
                conn.close()
                cur.close()
            conn = psycopg2.connect(database=model_name, user=user, password=password, host=host, port=port)
            return conn
        
    def load_model(self, model_path, model_name):
            """
            加载模型并将其转换为关系表存储在数据库中。

            Args:
                model_path (str): 模型文件的路径。
                model_name (str): 数据库模型的名称。
            """
            entity_inited = set()  # 记录已经初始化的实体
            try:
                # 创建数据库并连接
                conn = self.__createdb_for_model(self.__user, self.__password, self.__host, self.__port, model_name)
                print(f"Connected to database {model_name}.")
                print(f"Loading {model_path}...")
                
                # 将模型转换为关系表
                ifc_file = ifcopenshell.open(model_path)
                for entity in ifc_file:
                    entity_type = entity.is_a()
                    cursor = conn.cursor()
                    if(entity_type not in entity_inited):
                        # 创建实体表
                        entity_inited.add(entity_type)
                        command = self.__get_create_table_sql(entity)
                        cursor.execute(command)
                        conn.commit()
                        # print(command)
                    try:
                        # 插入属性记录
                        command = self.__get_insert_sql(entity)
                        cursor.execute(command)
                        # print(command)
                    except psycopg2.errors.UniqueViolation:
                        conn.rollback()
                    except Exception as e:
                        print(f"Error when inserting {entity}.")
                        print(e)
                        conn.rollback()
                        
                conn.commit()
                cursor.close()
                conn.close()
            except Exception as e:
                print(e)
