import heat_graph as hg
import json
import psycopg2 as ps

# Timer
import time

class Timer:
    def __init__(self):
        self.start_time = 0
        self.end_time = 0

    def start(self):
        self.start_time = time.time()

    def end(self, unit='s'):
        self.end_time = time.time()
        if unit == 's':
            return self.end_time - self.start_time
        elif unit == 'ms':
            return (self.end_time - self.start_time) * 1000
        elif unit == 'us':
            return (self.end_time - self.start_time) * 1000000

timer = Timer()

# 记录user度中心性
user_degree_centrality = {}
post_degree_centrality = {}

def sort_user_uuids():
    # 按照度中心性排序
    degree_sorted_users = sorted(user_degree_centrality.items(), key=lambda x: x[1], reverse=True)
    # 取出度中心性百分位数为10, 20, 40, 60, 80的用户
    user_uuids = [x[0] for x in degree_sorted_users]
    user_uuids_10 = user_uuids[int(len(user_uuids) * 0.1)]
    user_uuids_20 = user_uuids[int(len(user_uuids) * 0.2)]
    user_uuids_40 = user_uuids[int(len(user_uuids) * 0.4)]
    user_uuids_60 = user_uuids[int(len(user_uuids) * 0.6)]
    user_uuids_80 = user_uuids[int(len(user_uuids) * 0.8)]
    
    user_uuids = [user_uuids_10, user_uuids_20, user_uuids_40, user_uuids_60, user_uuids_80]
    return user_uuids

def load_fsnb_data(graph, dataset_path):
    uuid_to_id = {}
    vertex_id = 1
    vertex_num = 0
    edge_num = 0
    
    user_degree_centrality.clear()
    post_degree_centrality.clear()
    # 读取用户数据
    with open(f'{dataset_path}/users.json', 'r') as f:
        users = json.load(f)
        for user in users:
            uuid_to_id[user['id']] = vertex_id
            graph.add_vertex(vertex_id, user, ['User'])
            vertex_id += 1
            vertex_num += 1
    
    # 读取帖子数据
    with open(f'{dataset_path}/posts.json', 'r') as f:
        posts = json.load(f)
        for post in posts:
            uuid_to_id[post['id']] = vertex_id
            vertex_id += 1
            post_prop = {
                'id': post['id'],
                'content': post['content'],
            }
            graph.add_vertex(vertex_id, post_prop, ['Post'])
            vertex_num += 1
            creator_id = uuid_to_id[post['creator_id']]
            user_degree_centrality[post['creator_id']] = user_degree_centrality.get(post['creator_id'], 0) + 1
            post_id = uuid_to_id[post['id']]
            hyperedge_prop = {
                'created_at': post['created_at']
            }
            graph.add_hyperedge([post_id], [creator_id], 'created_by', hyperedge_prop)
            edge_num += 1
    
    # 读取评论数据
    with open(f'{dataset_path}/comments.json', 'r') as f:
        comments = json.load(f)
        for comment in comments:
            uuid_to_id[comment['id']] = vertex_id
            comment_id = vertex_id
            vertex_id += 1
            comment_prop = {
                'id': comment['id'],
                'content': comment['content'],
            }
            vertex_num += 1
            graph.add_vertex(vertex_id, comment_prop, ['Comment'])
            creator_id = uuid_to_id[comment['creator_id']]
            user_degree_centrality[comment['creator_id']] = user_degree_centrality.get(comment['creator_id'], 0) + 1
            post_id = uuid_to_id[comment['post_id']]
            post_degree_centrality[post['creator_id']] = post_degree_centrality.get(post['creator_id'], 0) + 1
            hyperedge_prop = {
                'created_at': comment['created_at']
            }
            graph.add_hyperedge([comment_id], [creator_id, post_id], 'related_to', hyperedge_prop)
            edge_num += 1
    
    # 读取关注关系
    with open(f'{dataset_path}/follows.json', 'r') as f:
        follows = json.load(f)
        for follow in follows:
            follower_id = uuid_to_id[follow['follower_id']]
            followee_id = [uuid_to_id[x] for x in follow['followee_id']]
            graph.add_hyperedge([follower_id], followee_id, 'follows', {})
            edge_num += 1
            user_degree_centrality[follow['follower_id']] = user_degree_centrality.get(follow['follower_id'], 0) + 1

def test_heat(dataset_path, test_name):
    # 连接数据库
    graph = hg.HEATGraph(db_name="heat_test", port=5433, user='zzm', password='66668888')

    # 清理上一次的测试数据
    # delete from public.heat_meta;
    # drop schema graph1 cascade;
    graph.drop_graph(f"{test_name}")

    # 创建超图
    graph.create_graph(f"{test_name}", 5)
    
    # 加载数据
    timer.start()
    load_fsnb_data(graph, dataset_path)
    print(f"heat load data: {timer.end('ms')} ms")
    graph.create_index()
    
    user_uuids = sort_user_uuids()
    print(f"heat user_uuids: {user_uuids}")
    percent = [10, 20, 40, 60, 80]
    # 
    for i in range(len(user_uuids)):
        user_uuid = user_uuids[i]
        percentage = percent[i]
        
        timer.start()
        result1 = graph.execute_cypher(f"""MATCH (p: Post)-[c: created_by]->(u:User {{"id": "{user_uuid}"}})""")
        # for r in result1[1]:
        #     print(r)
        print(f"heat match {percentage}% created_by: {timer.end('ms')} ms, result: {len(result1[1])}")

        timer.start()
        result2 = graph.execute_cypher(f"""MATCH (p: Comment)-[c: related_to]->(u:User {{"id": "{user_uuid}"}})""")
        print(f"heat match {percentage}% related_to: {timer.end('ms')} ms, result: {len(result2[1])}")
        
        timer.start()
        result1 = graph.execute_cypher(f"""MATCH (p: Post)-[c: created_by]->(u:User {{"id": "{user_uuid}"}})""", True)
        # for r in result1[1]:
        #     print(r)
        print(f"heat adj match {percentage}% created_by: {timer.end('ms')} ms, result: {len(result1[1])}")

        timer.start()
        result2 = graph.execute_cypher(f"""MATCH (p: Comment)-[c: related_to]->(u:User {{"id": "{user_uuid}"}})""", True)
        print(f"heat adj match {percentage}% related_to: {timer.end('ms')} ms, result: {len(result2[1])}")

def age_load_data(conn, graph_name):
    cursor = conn.cursor()
    user_degree_centrality.clear()
    post_degree_centrality.clear()
    vertex_num = 0
    edge_num = 0
    # 读取用户数据
    with open(f'{dataset_path}/users.json', 'r') as f:
        users = json.load(f)
        for user in users:
            sql = f"""SELECT * FROM cypher('{graph_name}', $$CREATE (u:User {{id: "{user['id']}", name: "{user['name']}", birthdate: "{user['birthdate']}", email: "{user['email']}"}}) RETURN u$$) AS (n agtype)
            """
            cursor.execute(sql)
            vertex_num += 1
            conn.commit()
            
    # 读取帖子数据
    with open(f'{dataset_path}/posts.json', 'r') as f:
        posts = json.load(f)
        for post in posts:
            creator = post['creator_id']
            sql = f"""SELECT * FROM cypher('{graph_name}', $$CREATE (p:Post {{id: "{post['id']}", content: "{post['content']}"}}) RETURN p$$) AS (n agtype)
            """
            cursor.execute(sql)
            vertex_num += 1
            conn.commit()
            cypher_sql = f"""MATCH (u:User {{id: "{creator}"}}), (p:Post {{id: "{post['id']}"}}) CREATE (p)-[e:created_by {{created_at: "{post['created_at']}"}}]->(u) RETURN e"""
            cursor.execute(f"""SELECT * FROM cypher('{graph_name}', $$ {cypher_sql} $$) AS (e agtype)""")
            edge_num += 1
            user_degree_centrality[creator] = user_degree_centrality.get(creator, 0) + 1
            conn.commit()
    #         

    
    # # 读取评论数据
    with open(f'{dataset_path}/comments.json', 'r') as f:
        comments = json.load(f)
        for comment in comments:
            commenter_id = comment['creator_id']
            post_id = comment['post_id']
            comment_id = comment['id']
            try:
                cypher_sql = f"""CREATE (c:Comment {{id: "{comment_id}", content: "{comment['content']}"}})"""
                sql = f"""SELECT * FROM cypher('{graph_name}', $$ {cypher_sql} $$) AS (n agtype)"""
                # print(sql)
                cursor.execute(sql)
                vertex_num += 1
                conn.commit()
                cypher_sql = f"""MATCH (u:User {{id: "{commenter_id}"}}), (c:Comment {{id: "{comment_id}"}}) CREATE (c)-[e:created_by {{created_at: "{comment['created_at']}"}}]->(u) RETURN e"""
                sql = f"""SELECT * FROM cypher('{graph_name}', $$ {cypher_sql} $$) AS (e agtype)"""
                # print(sql)
                cursor.execute(sql)
                conn.commit()
                cypher_sql = f"""MATCH (p:Post {{id: "{post_id}"}}), (c:Comment {{id: "{comment_id}"}}) CREATE (c)-[e:related_to]->(p) RETURN e"""
                cursor.execute(f"""SELECT * FROM cypher('{graph_name}', $$ {cypher_sql} $$) AS (e agtype)""")
                user_degree_centrality[commenter_id] = user_degree_centrality.get(commenter_id, 0) + 1
                post_degree_centrality[post_id] = post_degree_centrality.get(post_id, 0) + 1
                edge_num += 2
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
                
    # 读取关注关系
    with open(f'{dataset_path}/follows.json', 'r') as f:
        follows = json.load(f)
        for follow in follows:
            follower_id = follow['follower_id']
            for followee_id in follow['followee_id']:
                cypher_sql = f""" MATCH (u:User {{id: "{follow['follower_id']}"}}), (v:User {{id: "{follow['followee_id']}"}}) CREATE (u)-[e:follows]->(v) RETURN e"""
                cursor.execute(f"""SELECT * FROM cypher('{graph_name}', $$ {cypher_sql} $$) AS (e agtype)""")
                user_degree_centrality[follow['follower_id']] = user_degree_centrality.get(follow['follower_id'], 0) + 1
            edge_num += len(follow['followee_id'])
    cursor.close()

def test_age(dataset_path, test_name):
    # 连接数据库
    conn = ps.connect(dbname='age_test', host='localhost', port=5434, user='zzm', password='66668888')
    cursor = conn.cursor()
    cursor.execute("LOAD 'age'")
    cursor.execute("SET search_path = ag_catalog, \"zzm\", public;")
    # 
    graph_name = test_name
    cursor.execute(f"SELECT drop_graph('{graph_name}', true)")
    cursor.execute(f"SELECT create_graph('{graph_name}')")
    
    # age load data
    timer.start()
    age_load_data(conn, graph_name)
    print("age load data: ", timer.end('ms'), "ms")
    
    # 执行查询
    user_uuids = sort_user_uuids()
    print(f"age user_uuids: {user_uuids}")
    percent = [10, 20, 40, 60, 80]
    for i in range(len(user_uuids)):
        user_uuid = user_uuids[i]
        percentage = percent[i]
        # 1 to 1
        timer.start()
        sql = f"""SELECT * FROM cypher('{graph_name}', $$MATCH (p:Post)-[c:created_by]->(u:User {{id: "{user_uuid}"}}) RETURN p, c, u$$) AS (p agtype, c agtype, u agtype)"""
        # print(sql)
        cursor.execute(sql)
        result1 = cursor.fetchall()
        # for r in result1:
        #     print(r)
        print(f"match {percentage}% created_by: {timer.end('ms')} ms, result: {len(result1)}")
        # 1 to many
        timer.start()
        cursor.execute(f"""SELECT * FROM cypher('{graph_name}', $$MATCH (p:Comment)-[c:related_to]->(u:Post {{id: "{user_uuid}"}}) RETURN p, c, u$$) AS (p agtype, c agtype, u agtype)""")
        result2 = cursor.fetchall()
        sql = f"""SELECT * FROM cypher('{graph_name}', $$MATCH (p:Comment)-[c:created_by]->(u:User {{id: "{user_uuid}"}}) RETURN p, c, u$$) AS (p agtype, c agtype, u agtype)"""
        # print(sql)
        cursor.execute(sql)
        result3 = cursor.fetchall()
        print(f"match {percentage}% related_to: {timer.end('ms')} ms, result: {len(result2) + len(result3)}")
        
        
if __name__ == "__main__":
    sf = 1
    dataset_path = f'datasets/fsnb{sf}'
    test_age(dataset_path, f'age_fsnb{sf}')
    test_heat(dataset_path, f'heat_fsnb{sf}')