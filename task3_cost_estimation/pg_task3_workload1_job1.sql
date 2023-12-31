-- Job1: Calculate the total area of the site of the building
-- Output: total_site_area
SELECT SUM(CAST(ifcpropertysinglevalue."NominalValue" AS DOUBLE PRECISION))  as total_site_area
FROM (
    SELECT ifccovering.id, UNNEST(ifcpropertyset."HasProperties") as HasProperties
    FROM (
        -- 获取IfcBuildingStorey关联的IfcCovering的ID数组
        SELECT ifcbuildingstorey.id, ifcbuildingstorey."Name", UNNEST(ifcrelcontainedinspatialstructure."RelatedElements") as elements
        FROM ifcbuildingstorey 
        JOIN ifcrelcontainedinspatialstructure
        ON ifcrelcontainedinspatialstructure."RelatingStructure" = ifcbuildingstorey.id
    ) AS r1
    -- 获取IfcCovering实体
    JOIN ifccovering
    ON r1.elements = ifccovering.id
    JOIN (
        SELECT dbp1.id, UNNEST(dbp1."RelatedObjects") as related_id, dbp1."RelatingPropertyDefinition"
        FROM ifcreldefinesbyproperties as dbp1
        ) as pset1
    ON pset1.related_id = ifccovering.id
    -- 获取PropertySet中的数据
    JOIN ifcpropertyset
    ON  pset1."RelatingPropertyDefinition" = ifcpropertyset.id
    WHERE r1."Name" = 'Level 1' and ifcpropertyset."Name" = 'Dimensions'
) AS r2
-- 获取PropertySet中的Key-Value数据
JOIN ifcpropertysinglevalue
ON r2.hasproperties = ifcpropertysinglevalue.id
WHERE ifcpropertysinglevalue."Name" = 'Area';