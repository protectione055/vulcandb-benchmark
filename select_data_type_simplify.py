import os
import psycopg2
import json
import logging
import numpy as np

from util.common import Timer
import util.common as util

database_name = 'micro_benchmark'
json_table_name = 'json_comp_json'
jsonb_table_name = 'json_comp_jsonb'
raw_table_name = 'raw_base_type'

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

class JsonComp:
    
    def __init__(self):
        self._database_name = set()
        self._user = 'postgres'
        self._password = 'postgres'
        self._host = 'localhost'
        self._port = 5432
        
    def _get_json(self, data_type, value):
        data_type = data_type.lower() 
        return json.dumps({f"{data_type}": value})
    
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
        self._database_name.add(database_name)
        return conn
    
    # 创建新表，如果已存在则清空已有记录
    def _create_table(self, conn, table_name, create_table_query):
        try:
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(create_table_query)
        except psycopg2.errors.DuplicateTable:
            logger.warning(f"Table {table_name} already exists, deleting all records")
            try:
                cursor.execute(f'DELETE FROM {table_name}')
            except Exception as e:
                logger.error(f"Error when deleting records from {table_name}")
                logger.error(e)
        except Exception as e:
            logger.error(f"{create_table_query} failed")
            logger.error(e)
        finally:
            cursor.close()

    def _execute_query(self, conn, cursor, query, commit=True):
        try:
            cursor.execute(query)
            if commit:
                conn.commit()
        except psycopg2.errors.UniqueViolation:
            # conn.rollback()
            pass
        except Exception as e:
            conn.rollback()
            logger.error(f"Error when executing {query}")
            logger.error(e)

    @Timer.eclapse
    def prepare_jsonb(self, record_num, data_type='integer'):
        data_type = data_type.lower() 
        try:
            conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
            #print(f"Connected to database {database_name}.")
            
            table_name = jsonb_table_name
            create_table_query = f'CREATE TABLE {table_name} (data JSONB)'
            self._create_table(conn, table_name, create_table_query)

            # 插入一条记录
            conn.autocommit = False
            cursor = conn.cursor()
            for i in range(0, record_num):
                insert_json = self._get_json(data_type, i)
                command = f"INSERT INTO {table_name} (data) VALUES ('{insert_json}')"
                self._execute_query(conn, cursor, command, False)
            conn.commit()
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            conn.close()

    @Timer.eclapse
    def prepare_json(self, record_num, data_type='integer'):
        data_type = data_type.lower() 
        try:
            conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
            
            # 创建表
            table_name = json_table_name
            create_table_query = f'CREATE TABLE {table_name} (data JSON)'
            self._create_table(conn, table_name, create_table_query)
            
            # 插入记录
            conn.autocommit = False
            cursor = conn.cursor()
            for i in range(0, record_num):
                insert_json = self._get_json(data_type, i)
                command = f"INSERT INTO {table_name} (data) VALUES ('{insert_json}')"
                self._execute_query(conn, cursor, command, False)
            conn.commit()
        except Exception as e:
            logger.error(f"Error when preparing json records")
            print(e)
        finally:
            cursor.close()
            conn.close()
            
    @Timer.eclapse
    def prepare_raw(self, record_num, data_type='integer'):
        data_type = data_type.lower() 
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        
        # 创建表
        raw_command = f'CREATE TABLE {raw_table_name} (data {data_type})'
        self._create_table(conn, raw_table_name, raw_command)
            
        # 插入记录
        conn.autocommit = False
        for i in range(0, record_num):
            command = ""
            if data_type == 'integer' or data_type == 'oid' or data_type == 'real':
                command = f"INSERT INTO {raw_table_name} (data) VALUES ({i})"
            elif data_type == 'text':
                command = f"INSERT INTO {raw_table_name} (data) VALUES ('Basic Wall:Interior - Partition (92mm Stud):204300')"
            self._execute_query(conn, cursor, command, False)
        conn.commit()
        
    @Timer.eclapse
    def point_query_on_json(self, data_type, query_point):
        data_type = data_type.lower()
        table_name = json_table_name
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        if data_type == 'integer' or data_type == 'oid' or data_type == 'real':
            cursor.execute(f"""SELECT * FROM {table_name} WHERE (data->>'{data_type}')::{data_type} = {query_point}""")
        elif data_type == 'text':
            cursor.execute(f"""SELECT * FROM {table_name} WHERE data->>'text' = '{query_point}'""")
        rows = cursor.fetchall()
        # print(f"Query {len(rows)} records from {table_name}")
        cursor.close()
        conn.close()
        
    @Timer.eclapse
    def point_query_on_jsonb(self, data_type, query_point):
        data_type = data_type.lower()
        table_name = jsonb_table_name
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        if data_type == 'integer' or data_type == 'real' or data_type == 'oid':
            cursor.execute(f"""SELECT * FROM {table_name} WHERE data = '{{"{data_type}": {query_point}}}'""")
        elif data_type == 'text':
            cursor.execute(f"""SELECT * FROM {table_name} WHERE data = '{{"text": "{query_point}"}}'""")
        rows = cursor.fetchall()
        # print(f"Query {len(rows)} records from {table_name}")
        cursor.close()
        conn.close()
    
    @Timer.eclapse
    def point_query_on_raw(self, data_type, query_point):
        data_type = data_type.lower()
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        
        try:
            if data_type == 'integer' or data_type == 'oid':
                cursor.execute(f"""SELECT * FROM {raw_table_name} WHERE data = {query_point}""")
            elif data_type == 'real':
                cursor.execute(f"""SELECT * FROM {raw_table_name} WHERE data = {query_point}""")
            elif data_type == 'text':
                cursor.execute(f"""SELECT * FROM {raw_table_name} WHERE data = '{query_point}'""")
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(e)
        finally:
            cursor.close()
            conn.close()
    
    @Timer.eclapse
    def range_query_on_json(self, data_type, record_num, range_start, range_end):
        data_type = data_type.lower()
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        
        try:
            if data_type == 'integer' or data_type == 'oid':
                cursor.execute(f"""SELECT * FROM {json_table_name} WHERE (data->>'{data_type}')::integer BETWEEN {range_start} AND {range_end}""")
            elif data_type == 'real':
                cursor.execute(f"""SELECT * FROM {json_table_name} WHERE (data->>'{data_type}')::{data_type} BETWEEN {range_start} AND {range_end}""")
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(e)
        finally:
            cursor.close()
            conn.close()
    
    @Timer.eclapse
    def range_query_on_jsonb(self, data_type, record_num, range_start, range_end):
        data_type = data_type.lower()
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        
        try:
            if data_type == 'integer' or data_type == 'oid':
                cursor.execute(f"""SELECT * FROM {jsonb_table_name} WHERE (data->>'{data_type}')::integer BETWEEN {range_start} AND {range_end}""")
            elif data_type == 'real':
                cursor.execute(f"""SELECT * FROM {jsonb_table_name} WHERE (data->>'{data_type}')::{data_type} BETWEEN {range_start} AND {range_end}""")
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(e)
        finally:
            cursor.close()
            conn.close()
    
    @Timer.eclapse
    def range_query_on_raw(self, data_type, record_num, range_start, range_end):
        data_type = data_type.lower()
        
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        
        try:
            if data_type == 'integer' or data_type == 'oid':
                cursor.execute(f"""SELECT * FROM {raw_table_name} WHERE data BETWEEN {range_start} AND {range_end}""")
            elif data_type == 'real':
                cursor.execute(f"""SELECT * FROM {raw_table_name} WHERE data BETWEEN {range_start} AND {range_end}""")
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(e)
        finally:
            cursor.close()
            conn.close()
        
    @Timer.eclapse
    def aggregate_query_on_json(self, data_type):
        data_type = data_type.lower()
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        
        query = ""
        if data_type == 'integer':
            query = f"""SELECT SUM((data->>'{data_type}')::{data_type}) FROM {json_table_name}"""
        elif data_type == 'oid':
            query = f"""SELECT COUNT((data->>'{data_type}')::{data_type}) FROM {json_table_name}"""
        elif data_type == 'real':
            query = f"""SELECT SUM((data->>'{data_type}')::{data_type}) FROM {jsonb_table_name}"""
        self._execute_query(conn, cursor, query, True)
        rows = cursor.fetchall()
        cursor.close()
        
    @Timer.eclapse
    def aggregate_query_on_jsonb(self, data_type):
        data_type = data_type.lower()
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        
        query = ""
        if data_type == 'integer':
            query = f"""SELECT SUM((data->>'{data_type}')::{data_type}) FROM {jsonb_table_name}"""
        elif data_type == 'oid':
            query = f"""SELECT COUNT((data->>'{data_type}')::{data_type}) FROM {jsonb_table_name}"""
        elif data_type == 'real':
            query = f"""SELECT SUM((data->>'{data_type}')::{data_type}) FROM {jsonb_table_name}"""
        self._execute_query(conn, cursor, query, True)
        rows = cursor.fetchall()
        cursor.close()
        
    @Timer.eclapse
    def aggregate_query_on_raw(self, data_type):
        data_type = data_type.lower()
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        cursor = conn.cursor()
        
        query = ""
        if data_type == 'integer' or data_type == 'real':
            query = f"""SELECT SUM(data) FROM {raw_table_name}"""
        elif data_type == 'oid':
            query = f"""SELECT COUNT(data) FROM {raw_table_name}"""
        self._execute_query(conn, cursor, query, True)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        conn.close()
    
    def cleanup(self):
        conn = self._connect_to_db(self._user, self._password, self._host, self._port, database_name)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE json_comp_json")
        cursor.execute(f"DROP TABLE json_comp_jsonb")
        cursor.execute(f"DROP TABLE raw_base_type")
        cursor.close()
        conn.close()
        pass
    
    
if __name__ == '__main__': 
    json_comp = JsonComp()
    
    # 测试数据范围
    record_size = []
    start_record_size = 1000
    ratio = 2
    record_num = 10
    for i in range(0, record_num):
        record_size.append(start_record_size * (ratio ** i))
        
    # 测试范围查询选择率
    selectity = [0.1]
    
    # 测试数据类型
    data_types = ['OID', 'real']
    # record_size = [10000000]
    # data_types = ['INTEGER']
    for data_type in data_types:
        for record_num in record_size:
            # 写入数据
            json_comp.prepare_jsonb(record_num, data_type)
            json_comp.prepare_json(record_num, data_type)
            json_comp.prepare_raw(record_num, data_type)
            
            # 范围插叙测试
            for ratio in selectity:
                range_start = 1
                range_end = int(record_num * ratio)
                json_comp.range_query_on_jsonb(data_type, record_num, range_start, range_end)
                json_comp.range_query_on_json(data_type, record_num, range_start, range_end)
                json_comp.range_query_on_raw(data_type, record_num, range_start, range_end)
            
            # 聚合查询测试
            json_comp.aggregate_query_on_jsonb(data_type)
            json_comp.aggregate_query_on_json(data_type)
            json_comp.aggregate_query_on_raw(data_type)
            
            # TODO: 或许可以先建索引再查
            # 点查测试
            json_comp.point_query_on_jsonb(data_type, 1)
            json_comp.point_query_on_json(data_type, 1)
            json_comp.point_query_on_raw(data_type, 1)
            
            print(f"Record number: {record_num}, Data type: {data_type}")
            print("----------------------------------------------------")
        json_comp.cleanup()