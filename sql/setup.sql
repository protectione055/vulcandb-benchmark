-- Function: 从数据库中获取实体在属性集中的值
-- Input: entity_name, pset_name, property_name, condition
-- Output: eid, value_result
CREATE OR REPLACE FUNCTION get_pset_value(entity_name text, pset_name text, property_name text, condition text) RETURNS TABLE(eid INTEGER, value_result text) AS $$
DECLARE
    sql text;
BEGIN
    sql := format('
    SELECT r1.eid as eid, ifcpropertysinglevalue."NominalValue" AS value_result
    FROM (
        SELECT %s.id as eid, UNNEST(ifcpropertyset."HasProperties") as HasProperties
        -- 获取实体
        FROM %s
        JOIN (
            SELECT dbp1.id, UNNEST(dbp1."RelatedObjects") as related_id, dbp1."RelatingPropertyDefinition"
            FROM ifcreldefinesbyproperties as dbp1
            ) as pset1
        ON pset1.related_id = %s.id
        -- 获取PropertySet中的数据
        JOIN ifcpropertyset
        ON  pset1."RelatingPropertyDefinition" = ifcpropertyset.id
        WHERE ifcpropertyset."Name" = %L
    ) AS r1
    -- 获取PropertySet中的Key-Value数据
    JOIN ifcpropertysinglevalue
    ON r1.hasproperties = ifcpropertysinglevalue.id
    WHERE ifcpropertysinglevalue."Name" = %L AND %s;', entity_name, entity_name, entity_name, pset_name, property_name, condition);
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_bim_model(project_name text, project_desc text = '', schema_version text = 'IFC 4') RETURNS void AS $$
DECLARE
    uuid text;
    eid integer;
BEGIN
    -- 尝试创建名为project_name的schema, 如果已经存在则检查表中的记录是否为空, 如果不为空则抛出异常
    BEGIN
        -- TODO: 将这里替换成创建IfcProject实体的函数
        EXECUTE format('CREATE SCHEMA %s', project_name);
    EXCEPTION WHEN duplicate_schema THEN
            EXECUTE format('SELECT * FROM %s.ifcproject', project_name);
            IF FOUND THEN
                RAISE EXCEPTION 'Model %s already exists and is not empty', project_name;
            END IF;
        
    uuid := uuid_generate_v4();
    eid := nextval('ifc_instance_id_seq');

    INSERT INTO ifcproject (id, globalid, ownerhistory, name, description, creationdate, unitsincontext, longname, phase, representationcontext, units, compositiontype)
    VALUES format('%s, %s, NULL, %s, %s, NULL, NULL, NULL, NULL, NULL', eid, uuid, project_name, project_desc);
    END;

END;
$$ LANGUAGE plpgsql;
