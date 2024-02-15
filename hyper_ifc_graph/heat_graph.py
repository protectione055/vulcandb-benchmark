import logging
import psycopg2
import psycopg2.extras
import random
import json


import mmh3
import property_hypergraph as phg
import interference_graph as ig
from cypher_parser import CypherParser

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def removeprefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

schema_sql = """
CREATE OR REPLACE FUNCTION create_adjacency_tables(edge_width integer)
RETURNS VOID AS $$
DECLARE
    adj_col text;
    outgoing_sql text := 'CREATE TABLE outgoing_adj_table (vid BIGINT, overflow INTEGER DEFAULT 0';
    incomming_sql text := 'CREATE TABLE incoming_adj_table (vid BIGINT, overflow INTEGER DEFAULT 0';
BEGIN
    FOR i IN 1..edge_width LOOP
        adj_col := format(', HELBL%s TEXT, HEVAL%s JSONB', i::text, i::text);
        outgoing_sql := outgoing_sql || adj_col;
        incomming_sql := incomming_sql || adj_col;
    END LOOP; 
    outgoing_sql := outgoing_sql || ',PRIMARY KEY (vid, overflow))';
    incomming_sql := incomming_sql || ',PRIMARY KEY (vid, overflow))';
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
"""

class HEATGraph():
    def __init__(self, db_name = 'heat', host = 'localhost', port = 5432, user = 'postgres', password = 'postgres') -> None:
        self.db_name = db_name
        self.__host = host
        self.__port = port
        self.__user = user
        
        self.__cur_graph = ''
        self.__cur_adj_list_width = 0
        self.__coloring_hash = None
        self.__cur_hash_func1 = None
        self.__cur_hash_func2 = None
        try:
            self.__conn = psycopg2.connect(database = db_name, user = user, password = password, host = host, port = port)
            cursor = self.__conn.cursor()
            # 创建元数据表
            cursor.execute('CREATE TABLE IF NOT EXISTS public.heat_meta(hgid serial PRIMARY KEY, graph_name TEXT UNIQUE, adj_list_width INTEGER, use_coloring_hash BOOLEAN DEFAULT FALSE, hash_func1 TEXT,  hash_seed1 BIGINT, hash_func2 TEXT, hash_seed2 BIGINT)')
            # 创建着色表
            cursor.execute('CREATE TABLE IF NOT EXISTS public.heat_graph_coloring (hgid BIGINT, helbl TEXT, color BIGINT)')
        except psycopg2.errors.OperationalError:
            logger.error("Could not connect to database: " + "e")
            exit(1)
        finally:
            cursor.close()
            
    def create_graph(self, graph_name, adj_list_width, graph_coloring = None, hash_func = 'murmur3'):
        cursor = self.__conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {graph_name}')
            cursor.execute(f"SET search_path TO {graph_name}")
            # 初始化函数定义
            cursor.execute(schema_sql)
            
            # 初始化元数据表
            if graph_coloring is not None:
                logger.error("Graph coloring is not supported yet.")
                exit(1)
            else:
                hash_seed_1 = random.randint(0, 2**63)
                hash_seed_2 = random.randint(0, 2**63)
                self.__cur_hash_func1 = self.__gen_hash_func(hash_func, seed = hash_seed_1)
                self.__cur_hash_func2 = self.__gen_hash_func(hash_func, seed = hash_seed_2)
                cursor.execute(f'INSERT INTO public.heat_meta (graph_name, adj_list_width, use_coloring_hash, hash_func1, hash_seed1, hash_func2, hash_seed2) VALUES (\'{graph_name}\', {adj_list_width}, FALSE, \'{hash_func}\', {hash_seed_1}, \'{hash_func}\', {hash_seed_2})')
            cursor.execute(f'SELECT create_adjacency_tables({adj_list_width})')
            cursor.execute(f'SELECT create_vertex_table()')
            cursor.execute(f'SELECT create_hyperedge_table()')
            self.__conn.commit()
            self.open_graph(graph_name)
        except psycopg2.errors.DuplicateSchema:
            logger.warning(f"HyperGraph {graph_name} already exists.")
        except Exception as e:
            logger.error(e)
            exit(1)
        finally:
            cursor.close()
    
    def open_graph(self, graph_name):
        cursor = self.__conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(f"SELECT * FROM public.heat_meta WHERE graph_name = '{graph_name}'")
        if cursor.rowcount == 0:
            logger.error(f"HyperGraph {graph_name} does not exist.")
            raise ValueError(f"HyperGraph {graph_name} does not exist.")
        
        graph_meta = cursor.fetchone()
        self.__cur_adj_list_width = graph_meta['adj_list_width']
        self.__cur_graph = graph_name
        if graph_meta['use_coloring_hash']:
            logger.error("Graph coloring is not supported yet.")
            exit(1)
        else:
            self.__coloring_hash = None
            self.__cur_hash_func1 = self.__gen_hash_func(graph_meta['hash_func1'], seed = graph_meta['hash_seed1'])
            self.__cur_hash_func2 = self.__gen_hash_func(graph_meta['hash_func2'], seed = graph_meta['hash_seed2'])

        cursor.execute(f"SET search_path TO {graph_name}")
        cursor.close()
    
    def drop_graph(self, graph_name):
        cursor = self.__conn.cursor()
        try:
            # cursor.execute("SET search_path TO public")
            cursor.execute(f"DROP SCHEMA IF EXISTS {graph_name} CASCADE")
            cursor.execute(f"DELETE FROM public.heat_meta WHERE graph_name = '{graph_name}'")
            self.__conn.commit()
        finally:
            cursor.close()
        self.__cur_adj_list_width = 0
        self.__cur_graph = ''
        self.__coloring_hash = None
        self.__cur_hash_func1 = None
        self.__cur_hash_func2 = None
    
    def create_index(self):
        if self.__cur_graph == '':
            raise ValueError("No graph is open.")
        
        cursor = self.__conn.cursor()
        try:
            # index for vertex
            cursor.execute('CREATE INDEX vertex_vid_index ON vertex_table (vid)')
            cursor.execute('CREATE INDEX vertex_vlbl_index ON vertex_table (vlbl)')
            cursor.execute('CREATE INDEX vertex_prop_index ON vertex_table USING GIN (prop)')
            # index for hyperedge
            cursor.execute('CREATE INDEX hyperedge_outhv_index ON hyperedge_table (outhv)')
            cursor.execute('CREATE INDEX hyperedge_inhv_index ON hyperedge_table USING GIN (inhv)')
            # index for hyperedge_attr
            cursor.execute('CREATE INDEX hyperedge_attr_heid_index ON hyperedge_attr_table (heid)')
            cursor.execute('CREATE INDEX hyperedge_attr_helbl_index ON hyperedge_attr_table USING btree (helbl)')
            cursor.execute('CREATE INDEX hyperedge_attr_prop_index ON hyperedge_attr_table USING GIN (prop)')
            # index for outgoing_adj_table
            cursor.execute('CREATE INDEX outgoing_adj_vid_index ON outgoing_adj_table (vid)')
            # index for incoming_adj_table
            cursor.execute('CREATE INDEX incoming_adj_vid_index ON incoming_adj_table (vid)')
            self.__conn.commit()
        except Exception as e:
            logger.error(e)
            self.__conn.rollback()
        finally:
            cursor.close()
    
    def bulk_load_from_csv(self, csv_file):
        if self.__cur_graph == '':
            raise ValueError("No graph is open.")
    
    def add_vertex(self, vid = -1, prop = {}, labels = []):
        if self.__cur_graph == '':
            raise ValueError("No graph is open.")
        
        # 调用add_vertex函数，向数据库添加顶点
        prop_json = json.dumps(prop)
        array_elem = ','.join([f"'{item}'" for item in labels])
        label_array = f"ARRAY[{array_elem}]"
        cursor = self.__conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cursor.execute(f"INSERT INTO vertex_table VALUES ({vid}, {label_array}, '{prop_json}')")
        except psycopg2.errors.UniqueViolation:
            self.__conn.rollback()
            raise ValueError('Vertex already exists')
        except Exception as e:
            logger.error(e)
            self.__conn.rollback()
        finally:
            cursor.close()
        
    def add_hyperedge(self, source, target, label, prop = {}, heid = -1):
        if self.__cur_graph == '':
            raise ValueError("No graph is open.")

        cursor = self.__conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        prop_json = json.dumps(prop)
        # heid = 'DEFAULT' if heid == -1 else heid
        if heid == -1:
            cursor.execute(f"SELECT MAX(heid) FROM hyperedge_attr_table")
            fetch_result = cursor.fetchone()
            heid = 1 if fetch_result[0] is None else fetch_result[0] + 1
        
        try:
            # 插入hyperedge_table并获取返回的heid
            cursor.execute(f"INSERT INTO hyperedge_attr_table VALUES ({heid}, '{label}', '{prop_json}') RETURNING heid")
            # heid = cursor.fetchone()[0]
            
            # 将起始顶点集打散，并将对应的目标顶点集插入到hyperedge_table中
            inhv = f"ARRAY[{','.join([str(v) for v in target])}]"
            for s in source:
                cursor.execute(f"INSERT INTO hyperedge_table VALUES ({heid}, {s}, {inhv})")
                
            # 插入outgoing和incoming邻接表
            adj_id_1 = self.__cur_hash_func1(label) % self.__cur_adj_list_width + 1
            adj_id_2 = self.__cur_hash_func2(label) % self.__cur_adj_list_width + 1 
            
            # 更新邻接表
            def update_adj_table(table_name, adj_id_1, adj_id_2, vid, heid, heval, label):
                # 插入的位置使用(vid, overflow, adj_num)唯一标识
                select_sql = f"SELECT vid, overflow, helbl{adj_id_1}, heval{adj_id_1}, helbl{adj_id_2}, heval{adj_id_2} FROM {table_name} WHERE vid = {vid} ORDER BY overflow ASC"
                logger.debug(select_sql)
                cursor.execute(select_sql)
                adj_list_tuple = cursor.fetchall()
                logger.debug(adj_list_tuple)
                # 执行哈希函数，找到插入位置
                if len(adj_list_tuple) == 0:
                    # 如果当前顶点在邻接表中没有记录，则直接插入一条新纪录
                    cursor.execute(f"INSERT INTO {table_name} (vid, helbl{adj_id_1}, heval{adj_id_1}) VALUES ({vid}, '{label}', '{{\"{heid}\": {heval}}}')")
                else:
                    adj_id = ''
                    update_sql = ''
                    for i in range(len(adj_list_tuple)):
                        # 这里执行哈希函数，找到插入位置
                        update_value = f"""'{{"{heid}": {heval}}}'"""
                        if adj_list_tuple[i]['helbl' + str(adj_id_1)] == label:
                            adj_id = adj_id_1
                            update_value = f"""jsonb_set(heval{adj_id}, '{{"{heid}"}}', '{heval}', true)"""
                        elif adj_list_tuple[i]['helbl' + str(adj_id_2)] == label:
                            adj_id = adj_id_2
                            update_value = f"""jsonb_set(heval{adj_id}, '{{"{heid}"}}', '{heval}', true)"""
                        elif adj_list_tuple[i]['helbl' + str(adj_id_1)] == None:
                            adj_id = adj_id_1
                        elif adj_list_tuple[i]['helbl' + str(adj_id_2)] == None:
                            adj_id = adj_id_2
                        if adj_id != '':
                            update_sql = f"UPDATE {table_name} SET helbl{adj_id} = \'{label}\', heval{adj_id} = {update_value} WHERE vid = {vid} AND overflow = {i}"
                            break
                    if update_sql == '':
                        adj_id = adj_id_1
                        overflow_num = adj_list_tuple[-1]['overflow'] + 1
                        update_sql = f"INSERT INTO {table_name} (vid, overflow, helbl{adj_id}, heval{adj_id}) VALUES ({vid}, {overflow_num}, '{label}', '{{\"{heid}\": \"{heval}\"}}')"
                    logger.debug(update_sql)
                    cursor.execute(update_sql)
            # 写入outgoing_adj_table
            heval_str = removeprefix(inhv, 'ARRAY')
            for s in source:
                update_adj_table('outgoing_adj_table', adj_id_1, adj_id_2, s, heid, heval_str, label)
                
            # 写入incoming_adj_table
            for t in target:
                heval_str = f"[{','.join([str(s) for s in source])}]"
                update_adj_table('incoming_adj_table', adj_id_1, adj_id_2, t, heid, heval_str, label)
            self.__conn.commit()
        except psycopg2.errors.UniqueViolation:
            self.__conn.rollback()
            raise ValueError('Hyperedge already exists')
        except Exception as e:
            logger.error(e)
            self.__conn.rollback()
        finally:
            cursor.close()
    
    def match_vertex(self, label = '', prop = {}, var_name = ''):
        if self.__cur_graph == '':
            raise ValueError("No graph is open.")
        
        if var_name == '':
            var_name = 'vertex' + str(random.randint(0, 1000))
        
        cursor = self.__conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        prop_json = json.dumps(prop)
        
        # 匹配顶点的cte模板
        cte_template = f"""
        WITH vertex_cte AS (
            SELECT {var_name}.vid, {var_name}.vlbl, {var_name}.prop 
            FROM vertex_table AS {var_name}
            WHERE '{label}' ILIKE ANY({var_name}.vlbl) AND {var_name}.prop @> '{prop_json}'
        )
        """ if label != '' else f"""
        WITH vertex_cte AS (
            SELECT {var_name}.vid, {var_name}.vlbl, {var_name}.prop
            FROM vertex_table as {var_name}
            WHERE {var_name}.prop @> '{prop_json}'
        )
        """
        sql = cte_template + "SELECT * FROM vertex_cte"
        logger.debug(sql)
        cursor.execute(sql)
        return cursor.fetchall()
    
    def match_hyperedge(self, label = '', prop = {}, var_name = ''):
        if self.__cur_graph == '':
            raise ValueError("No graph is open.")
        
        if var_name == '':
            var_name = 'edge' + str(random.randint(0, 1000))
        
        cursor = self.__conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        prop_json = json.dumps(prop)
        
        # 匹配超边的cte模板
        cte_template = f"""
        WITH edge_cte AS (
            SELECT {var_name}.heid, {var_name}.helbl, {var_name}.prop 
            FROM hyperedge_attr_table AS {var_name}
            WHERE {var_name}.helbl = '{label}' AND {var_name}.prop @> '{prop_json}'
        )
        """ if label != '' else f"""
        WITH edge_cte AS (
            SELECT {var_name}.heid, {var_name}.helbl, {var_name}.prop
            FROM hyperedge_attr_table AS {var_name}
            WHERE {var_name}.prop @> '{prop_json}'
        )
        """
        sql = cte_template + "SELECT edge_cte.heid, edge_cte.helbl, edge_cte.prop, array_agg(ht.outhv), ht.inhv FROM edge_cte INNER JOIN hyperedge_table AS ht ON edge_cte.heid = ht.heid GROUP BY edge_cte.heid, edge_cte.helbl, edge_cte.prop, ht.inhv"
        logger.debug(sql)
        cursor.execute(sql)
        return cursor.fetchall()
    
    def translate_cypher(self, cypher_query, force_adj_hint = False):
        cypher_parser = CypherParser()
        (vertex_list, edge_list) = cypher_parser.parse(cypher_query)
        sql = ""
        if len(vertex_list) == 1 and len(edge_list) == 0:
            # 单顶点查询
            sql = self.__get_single_vertex_cte(vertex_list)
        else:
            # 路径查询
            cte_num = 1
            edge_num = len(edge_list)
            proj_list = "SELECT "
            vertex = vertex_list[0]
            sql = "WITH cte_0 AS ("
            proj_list = f"{vertex[0]}.vid, {vertex[0]}.vlbl, {vertex[0]}.prop"
            sql += self.__get_vertex_match_sql(vertex[0], vertex[1], vertex[2])
            
            # 如果查询的边数大于1，或者强制使用邻接表进行查询，则使用邻接表进行查询
            edge_match_generator = self.__get_edge_match_sql_from_adjtable
            if len(edge_list) == 1 and not force_adj_hint:
                edge_match_generator = self.__get_edge_match_sql_from_edgetable
            cte_num = 1
            for i in range(edge_num):
                edge = edge_list[i]
                edge_cte = edge_match_generator(edge[0], edge[1], edge[2])
                proj_list += f",{edge[0]}.heid, {edge[0]}.helbl, {edge[0]}.prop"
                vertex = vertex_list[i+1]
                vertex_cte = self.__get_vertex_match_sql(vertex[0], vertex[1], vertex[2])
                sql += f"""
                    ), cte_{cte_num} AS (
                        {edge_cte}
                    ), cte_{cte_num+1} AS (
                        {vertex_cte}
                """
                proj_list += f",{vertex[0]}.vid, {vertex[0]}.vlbl, {vertex[0]}.prop"
                cte_num += 2
            cte_num = 1
            sql += ") SELECT "
            sql += proj_list
            source = vertex_list[0]
            sql += f" FROM  cte_0 as {source[0]}"
            for i in range(len(vertex_list) - 1):
                source = vertex_list[i]
                edge = edge_list[i]
                target = vertex_list[i+1]
                sql += f" INNER JOIN cte_{cte_num} as {edge[0]}"
                sql += f" ON {source[0]}.vid = {edge[0]}.source"
                sql += f" INNER JOIN cte_{cte_num+1} as {target[0]}"
                sql += f" ON {edge[0]}.target = {target[0]}.vid"
                cte_num += 2

        return sql
    
    # 执行一个cypher-like的match查询
    # (n:Person {name: "John"})
    # (n:Person {name: "John"})-[e:knows]->(m:Person)
    # nb: 目前只支持左到右的单向查询
    def execute_cypher(self, cypher_query, force_adj_hint = False):
        sql = self.translate_cypher(cypher_query, force_adj_hint)
        try:
            logger.debug(sql)
            result = self.execute_sql(sql)
        except Exception as e:
            logger.error(e)
            return None
        return result
        
    def __get_single_vertex_cte(self, vertex_list):
        cte_num = 0
        vertex = vertex_list[0]
        match_vertex_cte =  self.__get_vertex_match_sql(vertex[0], vertex[1], vertex[2])
        match_vertex_cte = f"""
            WITH vertex_match_{cte_num} AS ({match_vertex_cte})
            SELECT * FROM vertex_match_{cte_num};
        """
        return match_vertex_cte
        
    def __get_vertex_match_sql(self, var_name: str, label: str, prop_json: str) -> str:
        sql = f"""
            SELECT {var_name}.vid, {var_name}.vlbl, {var_name}.prop 
            FROM vertex_table AS {var_name}
            WHERE '{label}' ILIKE ANY({var_name}.vlbl) AND {var_name}.prop @> '{prop_json}'
        """ if label != '' else f"""
            SELECT {var_name}.vid, {var_name}.vlbl, {var_name}.prop
            FROM vertex_table as {var_name}
            WHERE {var_name}.prop @> '{prop_json}'
        """
        return sql
    
    def __get_edge_match_sql_from_edgetable(self, var_name: str, label: str, prop_json: str) -> str:
        filter_clause = f"""
            WHERE '{label}' ILIKE {var_name}.helbl AND {var_name}.prop @> '{prop_json}'
        """ if label != '' else f"""
            WHERE {var_name}.prop @> '{prop_json}'
        """
        sql = f"""
            SELECT {var_name}.heid, {var_name}.helbl, {var_name}.prop, h.outhv AS source, unnest(h.inhv) AS target
            FROM hyperedge_attr_table AS {var_name} 
            INNER JOIN 
            hyperedge_table AS h 
            ON {var_name}.heid = h.heid 
            {filter_clause}
        """ if label != '' else f"""
            SELECT {var_name}.heid, {var_name}.helbl, {var_name}.prop, h.outhv AS source, unnest(h.inhv) AS target
            FROM hyperedge_attr_table AS {var_name} 
            INNER JOIN 
            hyperedge_table AS h 
            ON {var_name}.heid = h.heid 
            {filter_clause}
        """
        logger.debug(sql)
        return sql
    
    def __get_edge_match_sql_from_adjtable(self, var_name: str, label: str, prop_json: str) -> str:
        if self.__coloring_hash is not None:
            raise NotImplementedError("Graph coloring is not supported yet.")
        else:
            pos1 = self.__cur_hash_func1(label) % self.__cur_adj_list_width + 1
            pos2 = self.__cur_hash_func2(label) % self.__cur_adj_list_width + 1
            
            where_clause1 = f"""
                obt.helbl{pos1} = '{label}' 
                OR
                obt.helbl{pos2} = '{label}'
            """ if label != '' else '1=1'
            where_clause2 = f"""WHERE '{label}' ILIKE {var_name}.helbl AND {var_name}.prop @> '{prop_json}'
            """ if label != '' else """WHERE {var_name}.prop @> '{prop_json}'
            """
            
            sql = f"""
            SELECT out.heid, {var_name}.helbl, {var_name}.prop, out.vid AS source, jsonb_array_elements(out.inhv)::BIGINT AS target
            FROM 
            (
                    SELECT obt.vid, col.helbl, (jsonb_each(heval)).key::BIGINT AS heid, (jsonb_each(heval)).value AS inhv
                    FROM outgoing_adj_table AS obt
                    JOIN LATERAL 
                    (VALUES (vid, helbl{pos1}, heval{pos1}), (vid, helbl{pos2}, heval{pos2})) col(vid, helbl, heval) 
                    ON 
                    true
                    WHERE {where_clause1}
            ) AS out
            INNER JOIN
            hyperedge_attr_table AS {var_name}
            ON
            out.heid = {var_name}.heid
            {where_clause2}
            """
        logger.debug(sql)
        return sql
    
    # 执行SQL查询
    def execute_sql(self, query):
        if self.__cur_graph == '':
            raise ValueError("No graph is open.")

        try:
            cursor = self.__conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query)
            result = (cursor.description, cursor.fetchall())
        except Exception as e:
            logger.error(e)
            self.__conn.rollback()
            return None
        finally:
            cursor.close()
            
        return result
        
    
    def __gen_hash_func(self, hashfunc = 'murmur3', seed = 0):
        hashfunc = hashfunc.lower()
        if hashfunc == 'murmur3':
            return lambda x: mmh3.hash(x, seed)
        elif hashfunc == 'CityHash':
            raise NotImplementedError('CityHash is not supported yet')
        elif hashfunc == 'FarmHash':
            raise NotImplementedError('FarmHash is not supported yet')
        elif hashfunc == 'xxHash':
            raise NotImplementedError('xxHash is not supported yet')
        else:
            raise ValueError('Unknown hash function: ' + hashfunc)