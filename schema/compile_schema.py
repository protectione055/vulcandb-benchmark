import re
import os
import ifcopenshell

class PostbimCompilor:
    def __init__(self) -> None:
        pass
    
    def __get_create_table_sql(self, entity):
        pattern = re.compile(r'<bound method declaration.name of <entity (.*)>>')
        match = pattern.search(str(entity.name))
        entity_name = match.group(1)
        
        if entity.is_abstract():
            # print(f"Skipped abstract class: {entity_name}")
            return ""
        elif entity.as_enumeration_type():
            print(f"Skipped enumeration type: {entity_name}")
            exit(0)
            return ""

        create_table_sql = f"CREATE TABLE IF NOT EXISTS {entity_name} (id INTEGER PRIMARY KEY, "  # 创建表的SQL语句
        
        attributes = entity.all_attributes()    # TODO: 获取包含父类的所有属性
        for attr in attributes:
            print("-----")
            print(attr)
            ifc_named_type = attr.type_of_attribute()
            print(ifc_named_type)
            continue
            # 得到的字段类型以如下形式表示:
            # <attribute (.*): <(type|enumeration|entity|list|select|set|array) (.*)(: <(.*)>)?>>
            # ...<attribute ObjectType?: <type IfcLabel: <string>>>>
            # <attribute ImpliedOrder: <logical>>
            # <attribute OffsetValues: <array [1:2] of <type IfcLengthMeasure: <real>>>>
            # TODO: 需要更好的办法解析这东西
            attr_pattern = re.compile(r'<attribute (.*): <(type|enumeration|entity|select|list|set|array) (.*)(: <(.*)>)?>>')
            match = attr_pattern.search(str(attr))
            sql_type = ""
            
            if not match:
                # TODO: fix this
                attr_name = "Unknown"
                sql_type = "TEXT"
            else:
                # 将IFC中的类型转换为Postgresql中的类型
                attr_name = match.group(1)
                attr_type = match.group(2)
                type_name = match.group(3)      # 属性类型在IFC中的名称
                type_type = match.group(5)      # 属性类型在宿主语言中的类型
                attr_type_map = {
                    "integer": "INTEGER",
                    "real": "DOUBLE PRECISION",
                    "boolean": "BOOLEAN",
                    "logical": "VARCHAR(10)",
                    "string": "TEXT",
                }
                if entity_name == "ifcpropertysinglevalue" and attr_name == "NominalValue": # Special case:  IfcPropertySingleValue中的NominalValue字段可以是任意类型，因此直接看作TEXT
                    sql_type = "TEXT"
                elif attr_type == "enumeration":
                    sql_type = attr_name   # TODO: 为枚举类型生成创建枚举类型的SQL语句
                    # print(f"Enumeration type: {attr_name}")
                    sql_type = "TEXT"
                elif attr_type == "entity":     # 直接引用(实例ID)将存储为整数
                    sql_type = "INTEGER"
                    # TODO: sql_type += " REFERENCES " + type_name + "(id)"
                elif attr_type == "list":     # 选择类型
                    # TODO: 创建合适的列表类型，注意可能是一个二维列表, 或者是一个引用类型的列表
                    sql_type = "TEXT"
                elif attr_type == "set":     # 选择类型
                    # TODO: 创建合适的列表类型，注意可能是一个二维列表, 或者是一个引用类型的列表
                    sql_type = "TEXT"
                    pass
                elif attr_type == "select":     # 选择类型
                    # TODO: 在postgresql中如何模拟SELECT类型
                    sql_type = "TEXT"
                    pass
                elif attr_type == "type" and (type_type in attr_type_map):
                    sql_type = attr_type_map[type_type]
                else:
                    # print(f"Unknown attribute type: \033[33m{attr_type}\033[0m of type_type: \033[33m{type_type}\033[0m, when creating table for \033[33m{attr_name}\033[0m in \033[33m{entity_name}\033[0m.")
                    sql_type = "TEXT"
                
                # 如果字段名不是以问号结尾, 则说明该字段是必填字段
                if not attr_name.endswith("?"):
                    # 如果是, 则在字段类型后面加上NOT NULL
                    sql_type += " NOT NULL"
                else:
                    # 否则去掉字段名后面的问号
                    attr_name = attr_name[:-1]
            create_table_sql += f"\"{attr_name}\" {sql_type}," # 列名需要加引号，因为有些列名是SQL关键字(如: Outer)
        create_table_sql = create_table_sql[:-1] + ");"
        
        return create_table_sql

    def generate_sql_from_schema(self, schema_version):
        """
        生成在数据库中创建实体表的SQL函数
        ifcopenshell.express.parse有double free bug
        """            
        w = ifcopenshell.ifcopenshell_wrapper
        s = w.schema_by_name(schema_version)
        # print(s.entities()[0].all_attributes())
        entities = s.entities()
        create_sql = []
        failed_classes = []
        result_path = f"create_table_{schema_version}.sql"
        if os.path.exists(result_path):
            os.remove(result_path)
        with open(result_path, 'a') as r:
            for entity in entities:
                try:
                    sql_stmt = self.__get_create_table_sql(entity)
                    if sql_stmt == "":
                        continue
                    create_sql.append(sql_stmt)
                    r.write(sql_stmt + '\n')
                except Exception as e:
                    failed_classes.append(entity.name)
                    print(f"\033[31mERROR\033[0m when compiling \033[33m{entity.name}\033[0m")
                    print(e)
            print(f"Successfully compiled \033[32m{len(create_sql)}\033[0m entities.")
            print(f"Failed to compile \033[31m{len(failed_classes)}\033[0m entities.")
            # print(f"Failed entities: {failed_classes}")

if __name__ == '__main__':
    comp = PostbimCompilor()
    comp.generate_sql_from_schema("IFC4")
