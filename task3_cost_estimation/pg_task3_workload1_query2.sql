-- Job2: 计算地板面积之和
-- Output: total_slab_area
SELECT SUM(CAST(ifcpropertysinglevalue."NominalValue" AS DOUBLE PRECISION))  as total_site_area
FROM (
    SELECT ifccovering.id, UNNEST(ifcpropertyset."HasProperties") as HasProperties
    -- 获取IfcCovering实体
    FROM ifccovering
    JOIN (
        SELECT dbp1.id, UNNEST(dbp1."RelatedObjects") as related_id, dbp1."RelatingPropertyDefinition"
        FROM ifcreldefinesbyproperties as dbp1
        ) as pset1
    ON pset1.related_id = ifccovering.id
    -- 获取PropertySet中的数据
    JOIN ifcpropertyset
    ON  pset1."RelatingPropertyDefinition" = ifcpropertyset.id
    WHERE ifcpropertyset."Name" = 'Dimensions'
) AS r1
-- 获取PropertySet中的Key-Value数据
JOIN ifcpropertysinglevalue
ON r1.hasproperties = ifcpropertysinglevalue.id
WHERE ifcpropertysinglevalue."Name" = 'Area';
