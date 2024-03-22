CREATE OR REPLACE FUNCTION create_adjacency_tables(edge_width integer)
RETURNS VOID AS $$
DECLARE
    adj_col text;
    outgoing_sql text := 'CREATE TABLE outgoing_adj_table (vid BIGINT, overflow INTEGER';
    incomming_sql text := 'CREATE TABLE incoming_adj_table (vid BIGINT, overflow BOOLEAN DEFAULT FALSE';
BEGIN
    FOR i IN 1..edge_width LOOP
        adj_col := format(', %s TEXT, HEVAL%s JSONB', i::text, i::text);
        outgoing_sql := outgoing_sql || adj_col;
        incomming_sql := incomming_sql || adj_col;
    END LOOP; 
    outgoing_sql := outgoing_sql || 'PRIMARY KEY (vid, overflow))';
    incomming_sql := incomming_sql || 'PRIMARY KEY (vid, overflow)))';
    EXECUTE outgoing_sql;
    EXECUTE incomming_sql;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_vertex_table()
RETURNS VOID AS $$
BEGIN
    EXECUTE 'CREATE TABLE vertex_table (vid BIGINT PRIMARY KEY, vlbl TEXT[], prop JSONB)';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_hyperedge_table()
RETURNS VOID AS $$
BEGIN
    EXECUTE 'CREATE TABLE hyperedge_attr_table (heid BIGINT PRIMARY KEY, helbl TEXT, prop JSONB)';
    EXECUTE 'CREATE TABLE hyperedge_table (heid BIGINT, outhv BIGINT, inhv BIGINT[])';
END;
$$ LANGUAGE plpgsql;

-- 添加顶点
-- CREATE OR REPLACE FUNCTION add_vertex(vlbl text[], prop jsonb, vid BIGINTE DEFAULT NULL)
-- RETURNS VOID AS $$
-- BEGIN
--     -- TODO: 如果vid为空，则自动生成一个vid
--     EXECUTE format('INSERT INTO vertex_table (vid, vlbl, prop) VALUES (%s, %L, %L)', vid, vlbl, prop);
-- END;

-- -- @已弃用。逻辑太复杂，直接放到上层做
-- CREATE OR REPLACE FUNCTION add_hyperedge(heid integer, outhv integer[], inhv integer[], helbl text, prop jsonb, incolumn integer, outcolumn integer)
-- -- 添加超边
-- -- outhv: 出顶点集
-- -- inhv: 入顶点集
-- -- helbl: 超边标签
-- -- prop: 超边属性
-- -- incolumn: 入边列号序列，由调用者保证incolumn和inhv的长度相等
-- -- outcolumn: 出边列号，由调用者保证outcolumn和outhv的长度相等
-- RETURNS VOID AS $$
-- BEGIN
--     -- 检查头尾点集是否为空
--     IF array_length(outhv, 1) IS NULL THEN
--         RAISE EXCEPTION 'outhv is empty';
--     END IF;
--     IF array_length(inhv, 1) IS NULL THEN
--         RAISE EXCEPTION 'inhv is empty';
--     END IF;

--     -- 检查outhv和inhv是否有交集
--     IF outhv && inhv THEN
--         RAISE EXCEPTION 'outhv and inhv have intersection';
--     END IF;

--     -- 检查outhv和inhv中的出入顶点是否存在vertex_table中
--     IF EXISTS (SELECT 1 FROM unnest(outhv) AS outhvs WHERE NOT EXISTS (SELECT 1 FROM vertex_table WHERE vertex_table.vid = outhvs)) THEN
--         RAISE EXCEPTION 'outhv contains non-exist vertex';
--     END IF;
--     IF EXISTS (SELECT 1 FROM unnest(inhv) AS inhvs WHERE NOT EXISTS (SELECT 1 FROM vertex_table WHERE vertex_table.vid = inhvs)) THEN
--         RAISE EXCEPTION 'inhv contains non-exist vertex';
--     END IF;

--     -- 检查出入顶点是否存在adj_table中，如果不存在则创建
--     FOR i IN 1..array_length(outhv, 1) LOOP
--         IF NOT EXISTS (SELECT 1 FROM outgoing_adj_table WHERE vid = outhv[i]) THEN
--             EXECUTE format('INSERT INTO outgoing_adj_table (vid) VALUES (%s)', outhv[i]);
--         END IF;
--     END LOOP;
--     FOR i IN 1..array_length(inhv, 1) LOOP
--         IF NOT EXISTS (SELECT 1 FROM incoming_adj_table WHERE vid = inhv[i]) THEN
--             RAISE EXCEPTION 'incoming_adj_table does not contain vid %', inhv[i];
--             EXECUTE format('INSERT INTO incoming_adj_table (vid) VALUES (%s)', inhv[i]);
--         END IF;
--     END LOOP;

--     -- 将超边标签和属性插入到hyperedge_table中
--     EXECUTE format('INSERT INTO hyperedge_table (heid, helbl, prop) VALUES (%s, %L, %L)', heid, helbl, prop);

--     -- 将超边的起始顶点集打散并将对应的终止顶点集插入到hyperedge_adj_table和outgoing_adj_table中。如果该列上已经有值，则将新的值追加到该列上，否则向
--     FOR i IN 1..array_length(outhv, 1) LOOP
--         EXECUTE format('INSERT INTO hyperedge_adj_table (heid, outhv, inhv) VALUES (%s, %s, %s)', heid, outhv[i], inhv);
--         EXECUTE format('UPDATE outgoing_adj_table SET, HEVAL%s = HEVAL%s || {"%s": %s} WHERE vid = %s and ', outcolumn, helbl, outcolumn, outcolumn, inhv::text, outhv[i]);
--     END LOOP;

--     FOR i IN 1..array_length(outhv, 1) LOOP
--         EXECUTE format('INSERT INTO hyperedge_adj_table (heid, outhv, inhv) VALUES (%s, %s, %s)', heid, outhv, inhv[i]);
--     END LOOP;
-- END;

-- QUERY OPERATORS
-- 从ADJ_TABLE中获取所有的边
-- WITH unshred_edge AS (
--     SELECT test.vid, col.eid, col.label, col.tid from test JOIN LATERAL (VALUES (vid, eid1, label1, tid1), (vid, eid2, label2, tid2)) col(vid, eid, label, tid) ON true;
-- )