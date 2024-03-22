CREATE OR REPLACE FUNCTION import_ifc(file text)
RETURNS boolean
AS $$
import ifcopenshell
import os
class IfcImporter:
    def __init__(self):
        pass

    def __get_header(self, ifc_file):
        schema_version = ifc_file.schema_version()
        header = f"""ISO-10303-21;
        HEADER;
        FILE_DESCRIPTION(('ViewDefinition [CoordinationView]'),'2;1');
        FILE_NAME('','2019-03-13T10:59:41',(''),(''),'IfcOpenShell-0.6.0a1','IfcOpenShell-0.6.0a1','');
        FILE_SCHEMA(('{schema_version}'));
        ENDSEC;""".replace("'", "''").replace("    ", "")
        return header
    
    def _get_create_table_sql(self, entity, model_name):
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {model_name}.{entity.is_a()} (id INTEGER PRIMARY KEY, "  # 创建表的SQL语句
        attr_type_map = {
            "INT": "INTEGER",
            "DOUBLE": "DOUBLE PRECISION",
            "BOOL": "BOOLEAN",
            "LOGICAL": "VARCHAR(10)",
            "STRING": "VARCHAR(150)",
            "ENUMERATION": "JSON",
            "ENTITY INSTANCE": "VARCHAR(10)",
            "AGGREGATE OF ENTITY INSTANCE": "VARCHAR(10)[]",
            "AGGREGATE OF DOUBLE": "DOUBLE PRECISION[]",
            "AGGREGATE OF INT": "INTEGER[]",
            "AGGREGATE OF STRING": "text[]",
            "DERIVED": "JSON",
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

    def _get_insert_sql(self, entity, model_name):
        insert_sql = f"INSERT INTO {model_name}.{entity.is_a()} VALUES ({entity.id()},"
        attr_info = entity.get_info()
        
        for i in range(len(entity)):
            attr_name = entity.attribute_name(i)
            attr_type = entity.attribute_type(i)
            attr_value = attr_info[entity.attribute_name(i)]
            
            if entity.is_a("ifcpropertysinglevalue") and attr_name == "NominalValue":
                insert_sql += f"""'{str(attr_value.get_info()['wrappedValue']).replace("'", "''")}',"""
                continue
                    
            if attr_type == "DERIVED":
                continue
            if attr_value is None:
                insert_sql += "NULL,"
            elif attr_type == "DOUBLE" or attr_type == "INT":
                insert_sql += f"{attr_value},"
            elif attr_type == "ENUMERATION":
                insert_sql += f"'{attr_value}',"
            elif attr_type == "STRING" or attr_type == "LOGICAL":
                if isinstance(attr_value, str):
                    attr_value = attr_value.replace("'", "''")
                insert_sql += f"'{attr_value}',"
            elif attr_type == "BOOL":
                insert_sql += f"{attr_value},"
            elif attr_type == "ENTITY INSTANCE":
                # #123 -> 123
                attr_value = f"""'#{attr_value.id()}'"""
                insert_sql += f"{attr_value},"
            elif attr_type.startswith("AGGREGATE OF"):
                if attr_type == "AGGREGATE OF ENTITY INSTANCE":
                    # (#1,#2,#3) --> 'ARRAY['#1','#2','#3']
                    arg_str = ",".join([f"""'#{x.id()}'""" for x in attr_value])
                    attr_value = f"""ARRAY[{arg_str}],"""
                    insert_sql += attr_value
                    continue
                elif attr_type == "AGGREGATE OF STRING":
                    # ('a','b','c') --> '{"a","b","c"}'
                    attr_value = ['"' + x.replace('"', '""') + '"' for x in attr_value]
                attr_value = [str(x) for x in attr_value]
                insert_sql += f"""\'{{{",".join(attr_value)}}}',"""
        insert_sql = insert_sql[:-1] + ");"
        
        return insert_sql

    def prepare_data(self, ifc_file):
        entity_inited = set()  # 记录已经初始化的实体
        plpy.execute("CREATE TABLE IF NOT EXISTS ifc_meta (model_name VARCHAR(50) UNIQUE, header TEXT)")
        try:
            # 创建数据库并连接
            model_name = os.path.basename(ifc_file).split(".")[0]
            print(f"Loading {ifc_file}...")
            plpy.execute(f'DROP SCHEMA IF EXISTS {model_name} CASCADE')
            plpy.execute(f'CREATE SCHEMA {model_name}')
            # 将模型转换为关系表
            ifc_file = ifcopenshell.open(ifc_file)
            header = self.__get_header(ifc_file)
            plpy.execute(f"INSERT INTO ifc_meta VALUES ('{model_name}', '{header}')")
            for entity in ifc_file:
                entity_type = entity.is_a()
                if(entity_type not in entity_inited):
                    # 创建实体表
                    entity_inited.add(entity_type)
                    command = self._get_create_table_sql(entity, model_name)
                    plpy.notice(command)
                    plpy.execute(command)
                try:
                    # 插入属性记录
                    command = self._get_insert_sql(entity, model_name)
                    plpy.notice(command)
                    plpy.execute(command)
                except Exception as e:
                    print(f"Error when inserting {entity}.")
                    plpy.notice(e)
                    return False
        except Exception as e:
            plpy.error(e)
            return False
        return True

importer = IfcImporter()
return importer.prepare_data(file)
$$ LANGUAGE plpython3u;

-- 导出IFC
CREATE OR REPLACE FUNCTION export_ifc(model_name text, dest_path text)
RETURNS boolean
AS $$
import ifcopenshell
import os
import re

def export(model_name, dest_path, schema_version="IFC4"):
    try:
        id_pattern = re.compile(r"#(\d+)")
        with open(os.path.join(dest_path, model_name+".ifc"), "w") as f:
            header = plpy.execute(f"SELECT header FROM ifc_meta WHERE model_name='{model_name}'")[0]['header']
            f.write(header + '\n')
            result = plpy.execute(f"SELECT tablename FROM pg_tables WHERE schemaname='{model_name}' AND tablename LIKE 'ifc%'")     #查询schema为model_name中的所有表名
            tables = [row['tablename'] for row in result]
            f.write('DATA;\n')
            for entity in tables:
                rows = plpy.execute(f'SELECT * FROM {model_name}."{entity}"')
                for row in rows:
                    # 导出属性列表
                    args = ""
                    for key, value in row.items():
                        if key == "id":
                            continue
                        if type(value) == list:
                            args += f"({','.join([str(x) for x in value])}),"
                        elif type(value) == str and not id_pattern.match(value):
                            args += f"'{value}',"
                        elif value is None:
                            args += "$,"
                        else:
                            args += f"{value},"
                    args = args[:-1]
                    ifc_entity = f"""#{row['id']}={str(entity).upper()}({args})"""
                    plpy.notice(f"{ifc_entity}")
                    f.write(ifc_entity + ';\n')
            f.write('ENDSEC;\n')
            f.write('END-ISO-10303-21;\n')
        return True
    except Exception as e:
        plpy.notice(e)
        return False

return export(model_name, dest_path)
$$ LANGUAGE plpython3u;