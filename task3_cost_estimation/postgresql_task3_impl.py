import os
import psycopg2
import ifcopenshell
import ifcopenshell.util.selector as selector
import ifcopenshell.util.element

from util.common import Timer
from task3_cost_estimation.task3 import CostEstimator
import util.common as util

class PGTask3Impl:
    
    def __init__(self, args):
        self._database_name = []
        self._user = args["user"]
        self._password = args["password"]
        self._host = args["host"]
        self._port = args["port"]
        
    def _get_create_table_sql(self, entity):
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
    
    def _get_insert_sql(self, entity):
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

    def _connect_to_db(self, user, password, host, port, database_name):
        try:
            conn = psycopg2.connect(database="postgres", user=user, password=password, host=host, port=port)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f"CREATE DATABASE {database_name}")
        except psycopg2.errors.DuplicateDatabase:
            pass
        except Exception as e:
            raise e
        finally:
            conn.close()
            cur.close()
        conn = psycopg2.connect(database=database_name, user=user, password=password, host=host, port=port)
        return conn
        
    @Timer.eclapse
    def prepare_data(self, workloads):
        entity_inited = set()  # 记录已经初始化的实体
        try:
            for workload in workloads:
                # 创建数据库并连接
                model_name = os.path.basename(workload).split(".")[0]
                self._database_name.append(model_name)
                conn = self._connect_to_db(self._user, self._password, self._host, self._port, model_name)
                print(f"Connected to database {model_name}.")
                print(f"Loading {workload}...")
                # 将模型转换为关系表
                ifc_file = ifcopenshell.open(workload)
                for entity in ifc_file:
                    entity_type = entity.is_a()
                    cursor = conn.cursor()
                    if(entity_type not in entity_inited):
                        # 创建实体表
                        entity_inited.add(entity_type)
                        command = self._get_create_table_sql(entity)
                        cursor.execute(command)
                        conn.commit()
                        # print(command)
                    try:
                        # 插入属性记录
                        command = self._get_insert_sql(entity)
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
        pass

    @Timer.eclapse
    def run_job1(self):
        cost_result = CostEstimator()
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, "workload1")
        cur = conn.cursor()
        
        # 1. 通过一楼所有的地板面积之和计算场地总面积
        # 从pg_task3_workload1.sql读取SQL语句
        query1 = open("task3_cost_estimation/pg_task3_workload1.sql", "r").read()
        cur.execute(query1)
        total_site_area = util.square_unit_transform(cur.fetchone()[0], "ft^2")
        cost_result.set_site_area(total_site_area)
        
        # 2. 计算地板面积之和
        query2 = open("task3_cost_estimation/pg_task3_workload2.sql", "r").read()
        cur.execute(query2)
        total_slab_area = util.square_unit_transform(cur.fetchone()[0], "ft^2")
        cost_result.set_slab_area(total_slab_area)
        
        return cost_result
    
    def cleanup(self):
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, self._user)
        conn.autocommit = True
        for database_name in self._database_name:
            cursor = conn.cursor()
            cursor.execute(f"DROP DATABASE {database_name}")
            conn.commit()
            cursor.close()
        print(f"Cleaned up {len(self._database_name)} databases.")
        conn.close()
        pass