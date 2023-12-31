from ifcopenshell import express

def generate_entity_table_functions(exp_file):
    """
    生成在数据库中创建实体表的SQL函数
    """
    try:
        schema_version = exp_file.split('.')[0]
        mapping = express.parse(exp_file)
        print(mapping.schema.entities)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    generate_entity_table_functions("IFC4.exp")
