import heat_graph as hg

# 连接数据库
graph = hg.HEATGraph(db_name="heat_test", port=5433, user='zzm', password='66668888')

# 清理上一次的测试数据
# delete from public.heat_meta;
# drop schema graph1 cascade;
graph.drop_graph('graph1')

# 创建超图
graph.create_graph('graph1', 10)

# 添加顶点
graph.add_vertex(1, {'name': 'peter'}, ['Person', 'Student'])
graph.add_vertex(2, {'name': 'mary'}, ['Person', 'Teacher'])
graph.add_vertex(3, {'name': 'john'}, ['Person', 'Worker'])
graph.add_vertex(4, {'name': 'lucy'}, ['Person', 'Worker'])
graph.add_vertex(5, {'name': 'lily'}, ['Person', 'Professor'])
graph.add_vertex(6, {'name': 'tom'}, ['Person', 'teach assistant'])
graph.add_vertex(7, {'name': 'jerry'}, ['Person', 'headmaster'])
graph.add_vertex(8, {'name': 'bob'}, ['Person', 'Student'])
graph.add_vertex(9, {'name': 'alice'}, ['Person', 'Student'])
graph.add_vertex(10, {'name': 'Shenzhen University'}, ['School'])
graph.add_vertex(11, {'name': 'Tsinghua University'}, ['School'])
graph.add_vertex(12, {'name': 'Zhaoqing High School'}, ['School'])
# 添加超边
graph.add_hyperedge([1, 2], [11, 12], 'graduatedFrom')
graph.add_hyperedge([3, 4], [10, 12], 'graduatedFrom')
graph.add_hyperedge([12], [4], 'buildBy', {'year': '2005'})
graph.add_hyperedge([3, 2, 1], [4, 5, 6], 'knows', {'since': '2019-01-01'})
graph.add_hyperedge([1, 2, 3], [9, 8, 7], 'knows', {'since': '2020-01-01'})
graph.add_hyperedge([4, 5, 6], [7, 8, 9], 'knows')
graph.add_hyperedge([1, 2, 3], [9, 8, 7], 'partner')
graph.add_hyperedge([1, 2, 3], [9, 8, 7], 'friends')
graph.add_hyperedge([1, 2, 3], [4, 5, 7], 'workmate')
graph.add_hyperedge([1, 2, 3], [5, 6, 8], 'classmate')
graph.add_hyperedge([1, 2, 3], [9, 8, 7], 'roommate')
graph.add_hyperedge([1, 2, 3], [9, 8, 7], 'teammate')
graph.add_hyperedge([1, 2, 3], [4,5,6], 'teammate')

# 查询指定顶点
vertex = graph.match_vertex('teacher', {'name': 'mary'})
for t in vertex:
    print(t)

# 查询所有顶点
vertex = graph.match_vertex()
for t in vertex:
    print(t)

# 查询指定超边
hyperedge = graph.match_hyperedge('knows', {'since': '2019-01-01'})
for t in hyperedge:
    print(t)

# 查询所有超边
hyperedge = graph.match_hyperedge()
for t in hyperedge:
    print(t)
    

adj_hint = False
# 顶点查询 QUERY1
#
# PGQL
# SELECT *
# FROM MATCH (n:Person)
# WHERE n.name = 'peter'
#
# Cypher
# MATCH (n:Person {"name": "peter"})
print('=====================================')
print('                QUERY1               ')
print('=====================================')
query1 = 'MATCH (n:Person {"name": "peter"})'
result1 = graph.execute_cypher(query1, adj_hint)
column_names = [desc[0] for desc in result1[0]]
print(column_names)
for t in result1[1]:
    print(t)

# 路径查询 QUERY2
#
# PGQL
# SELECT *
# FROM MATCH (n:Person)-[e:knows]->(m:Person)
# WHERE n.name = 'peter' AND e.since = '2020-01-01' AND m.name = 'jerry'
#
# Cypher:
# MATCH (n:Person {"name": "peter"})-[e:knows {"since": "2020-01-01"}]->(m:Person {"name": "jerry"}
print('=====================================')
print('                QUERY2               ')
print('=====================================')
query2 = 'MATCH (n:Person {"name": "peter"})-[e:knows {"since": "2020-01-01"}]->(m:Person {"name": "jerry"})'
result2 = graph.execute_cypher(query2, adj_hint)
column_names = [desc[0] for desc in result2[0]]
print(column_names)
for t in result2[1]:
    print(t)

# 路径查询 QUERY3
#
# PGQL
# SELECT *
# FROM MATCH (n:Teacher)-[e:graduatedFrom]->(m:School)
# WHERE m.name = 'Zhaoqing High School'
#
# Cypher:
# MATCH (n:Teacher)-[e:graduatedFrom]->(m:School {"name": "Zhaoqing High School"})
print('=====================================')
print('                QUERY3               ')
print('=====================================')
query3 = 'MATCH (n:Teacher)-[e:graduatedFrom]->(m:School {"name": "Zhaoqing High School"})'
result3 = graph.execute_cypher(query3, adj_hint)
column_names = [desc[0] for desc in result3[0]]
print(column_names)
for t in result3[1]:
    print(t)
    
    
# 路径查询 QUERY4
#
# PGQL
# SELECT *
# FROM MATCH (n:Teacher)-[e:graduatedFrom]->(m:School)
# WHERE m.name = 'Zhaoqing High School'
#
# Cypher:
# MATCH (n:Teacher)-[e:graduatedFrom]->(m:School {"name": "Zhaoqing High School"})
print('=====================================')
print('                QUERY4               ')
print('=====================================')
query4 = 'MATCH (p:Person)-[e:graduatedFrom]->(m:School)-[f:buildBy]->(w:Worker {"name": "lucy"})'
result4 = graph.execute_cypher(query4, adj_hint)
column_names = [desc[0] for desc in result4[0]]
print(column_names)
for t in result4[1]:
    print(t)

# print(graph.translate_cypher(query1, adj_hint))
# print(graph.translate_cypher(query2, adj_hint))
# print(graph.translate_cypher(query3, adj_hint))
# print(graph.translate_cypher(query4, adj_hint))



'''
TODO: 对度中心性不同的顶点做BFS，比较效率
=====================================
                QUERY1               
=====================================
['vid', 'vlbl', 'prop']
[1, ['Person', 'Student'], {'name': 'peter'}]
=====================================
                QUERY2               
=====================================
['vid', 'vlbl', 'prop', 'heid', 'helbl', 'prop', 'vid', 'vlbl', 'prop']
[1, ['Person', 'Student'], {'name': 'peter'}, 4, 'knows', {'since': '2020-01-01'}, 7, ['Person', 'headmaster'], {'name': 'jerry'}]
=====================================
                QUERY3               
=====================================
['vid', 'vlbl', 'prop', 'heid', 'helbl', 'prop', 'vid', 'vlbl', 'prop']
[2, ['Person', 'Teacher'], {'name': 'mary'}, 1, 'graduatedFrom', {}, 12, ['School'], {'name': 'Zhaoqing High School'}]
=====================================
                QUERY4               
=====================================
['vid', 'vlbl', 'prop', 'heid', 'helbl', 'prop', 'vid', 'vlbl', 'prop', 'heid', 'helbl', 'prop', 'vid', 'vlbl', 'prop']
[1, ['Person', 'Student'], {'name': 'peter'}, 1, 'graduatedFrom', {}, 12, ['School'], {'name': 'Zhaoqing High School'}, 3, 'buildBy', {'year': '2005'}, 4, ['Person', 'Worker'], {'name': 'lucy'}]
[2, ['Person', 'Teacher'], {'name': 'mary'}, 1, 'graduatedFrom', {}, 12, ['School'], {'name': 'Zhaoqing High School'}, 3, 'buildBy', {'year': '2005'}, 4, ['Person', 'Worker'], {'name': 'lucy'}]
[3, ['Person', 'Worker'], {'name': 'john'}, 2, 'graduatedFrom', {}, 12, ['School'], {'name': 'Zhaoqing High School'}, 3, 'buildBy', {'year': '2005'}, 4, ['Person', 'Worker'], {'name': 'lucy'}]
[4, ['Person', 'Worker'], {'name': 'lucy'}, 2, 'graduatedFrom', {}, 12, ['School'], {'name': 'Zhaoqing High School'}, 3, 'buildBy', {'year': '2005'}, 4, ['Person', 'Worker'], {'name': 'lucy'}]
'''