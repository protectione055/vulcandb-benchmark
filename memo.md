## gather_edge实现
-- 需要从表1中将`eid1, eid2, eid3数组与label1, label2, label3数组各自关联起来，并将它们展开成行。每个eid数组应该与相应的label数组项关联。这可以通过使用unnest函数在PostgreSQL中实现，并且需要确保数组中的每个eid项与相应的label`项匹配。
-- vid | eid | label
-- ----|-----|------
-- 9   | 1   | edge1
-- 9   | 2   | edge1
-- 9   | 3   | edge1
-- 9   | 4   | edge2
-- 9   | 5   | edge2
-- 9   | 6   | edge2
-- 9   | 7   | edge3
-- 9   | 8   | edge3

-- 行折叠技术：将多个结构相同的列合并成一个列
-- vid | eid | label
-- ----|-----|------
9   | {1,2,3}   | edge1
9   | {4,5,6}   | edge2
9   | {7,8}   | edge3
SELECT test.vid, col.eid, col.label from test JOIN LATERAL (VALUES (vid, eid1, label1), (vid, eid2, label2)) col(vid, eid, label) ON true;

### 行折叠性能测试
#### 1. 使用LATERAL，带有WEHRE
EXPLAIN ANALYZE SELECT test.vid, col.eid, col.label, col.tid from test JOIN LATERAL (VALUES (vid, eid1, label1, tid1), (vid, eid2, label2, tid2)) col(vid, eid, label, tid) ON true WHERE test.vid = 100;

1. 数据量100
 Planning Time: 0.191 ms
 Execution Time: 0.114 ms

2. 数据量1000
 Planning Time: 0.884 ms
 Execution Time: 0.356 ms

3. 数据量10000
 Planning Time: 0.186 ms
 Execution Time: 3.664 ms

4. 数据量100000
 Planning Time: 0.319 ms
 Execution Time: 23.949 ms

5. 数据量1000000
 Planning Time: 0.208 ms
 Execution Time: 84.297 ms

#### 2. 使用UNION，带有WHERE
EXPLAIN ANALYZE SELECT COUNT(*) FROM (SELECT vid, eid1, label1, tid1 FROM test where vid = 100 UNION ALL SELECT vid, eid2, label2, tid2 FROM test WHERE vid = 100 UNION ALL SELECT vid, eid3, label3, tid3 FROM test WHERE vid = 100) t;

1. 数据量100
 * Planning Time: 0.331 ms
 * Execution Time: 0.177 ms

2. 数据量1000
* Planning Time: 0.228 ms
* Execution Time: 0.871 ms

3. 数据量10000
* Planning Time: 0.293 ms
* Execution Time: 7.055 ms

4. 数据量100000
* Planning Time: 0.243 ms
* Execution Time: 27.451 ms

5. 数据量1000000
* Planning Time: 0.266 ms
* Execution Time: 186.652 ms

#### 3. 使用jsonb
            SELECT out.vid AS source, 
            FROM 
            (
                    SELECT obt.vid, col.helbl, (jsonb_each(heval)).key AS heid, (jsonb_each(heval)).value AS inhv
                    FROM outgoing_adj_table AS obt
                    JOIN LATERAL 
                    (VALUES (vid, helbl1, heval1), (vid, helbl2, heval2)) col(vid, helbl, heval) 
                    ON 
                    true ;
            ) AS out
            INNER JOIN
            hyperedge_table AS ht
            ON
            out.heid = ht.heid

## LDBC-SNB

### Query1

```Gremlin
g.V().hasLabel('Person').has('id', personId).out('knows').as('friend')
    .order().by('lastName', incr).by('firstName', incr)
    .select('friend').out('created').hasLabel('Post').order().by('creationDate', decr)
    .limit(20).as('post')
    .select('friend', 'post')
    .by(valueMap('firstName', 'lastName'))
    .by(valueMap('id', 'imageFile', 'creationDate', 'locationIP', 'browserUsed', 'content', 'length'))
```