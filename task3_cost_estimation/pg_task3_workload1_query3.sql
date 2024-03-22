-- Job3: 计算内外墙面积之和
-- Output: total_interior_area, total_exterior_area

-- Function: get_pset_value
-- Input: table_name, pset_name, key_name, condition
-- Output: eid, value_result
CREATE OR REPLACE FUNCTION get_pset_value(table_name text, pset_name text, key_name text, condition text) RETURNS TABLE(eid INTEGER, value_result text) AS $$
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
    WHERE ifcpropertysinglevalue."Name" = %L AND %s;', table_name, table_name, table_name, pset_name, key_name, condition);
    RAISE NOTICE '%' , sql;
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

-- ##
-- 计算内墙总面积
SELECT r1.sum + r2.sum AS total_interior_wall_area
FROM 
(
    SELECT SUM(CAST(t1.value_result AS DOUBLE PRECISION)) AS sum
    FROM get_pset_value('ifcwall', 'Dimensions', 'Area', '1=1') AS t1
    JOIN get_pset_value('ifcwall', 'Pset_WallCommon', 'IsExternal', 'ifcpropertysinglevalue."NominalValue"=''False''') AS t2
    ON t1.eid = t2.eid
) AS r1,
(
    SELECT SUM(CAST(t3.value_result AS DOUBLE PRECISION)) AS sum
    FROM get_pset_value('ifcwallstandardcase', 'Dimensions', 'Area', '1=1') AS t3
    JOIN get_pset_value('ifcwallstandardcase', 'Pset_WallCommon', 'IsExternal', 'ifcpropertysinglevalue."NominalValue"=''False''') AS t4
    ON t3.eid = t4.eid
) AS r2;

-- ##
-- 计算外墙总面积
SELECT r1.sum + r2.sum AS total_exterior_wall_area
FROM 
(
    SELECT SUM(CAST(t1.value_result AS DOUBLE PRECISION)) AS sum
    FROM get_pset_value('ifcwall', 'Dimensions', 'Area', '1=1') AS t1
    JOIN get_pset_value('ifcwall', 'Pset_WallCommon', 'IsExternal', 'ifcpropertysinglevalue."NominalValue"=''True''') AS t2
    ON t1.eid = t2.eid
) AS r1,
(
    SELECT SUM(CAST(t3.value_result AS DOUBLE PRECISION)) AS sum
    FROM get_pset_value('ifcwallstandardcase', 'Dimensions', 'Area', '1=1') AS t3
    JOIN get_pset_value('ifcwallstandardcase', 'Pset_WallCommon', 'IsExternal', 'ifcpropertysinglevalue."NominalValue"=''True''') AS t4
    ON t3.eid = t4.eid
) AS r2;
