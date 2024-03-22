import unittest
from unittest.mock import MagicMock
from cypher_parser import CypherParser

class HEATGraphTest(unittest.TestCase):
    def setUp(self):
        self.parser = CypherParser()
        
    def tearDown(self) -> None:
        pass
    
    def test_single_vertex_match(self):
        sql = self.parser.parse('MATCH (n:Person {"name": "peter"})')
        expected_sql = '''
        WITH vertex_match_0 AS (
            SELECT n.vid, n.vlbl, n.prop 
            FROM vertex_table AS n
            WHERE 'Person' ILIKE ANY(n.vlbl) AND n.prop @> '{"name": "peter"}'
        )
        SELECT * FROM vertex_match_0;'''
        sql = sql.replace('\n', '').replace(' ', '').lower()
        expected_sql = expected_sql.replace('\n', '').replace(' ', '').lower()
        # print(sql)
        # print(expected_sql)
        self.assertEqual(sql, expected_sql)

    def test_parse_path_match(self):
        sql = self.parser.parse('MATCH (n:Person {"name": "peter"})-[e:knows {"since": "2020-01-01"}]->(m:Person)')
        expected_sql = '''
            WITH cte_0 AS (
                SELECT n.vid, n.vlbl, n.prop
                FROM vertex_table AS n
                WHERE 'Person' ILIKE ANY(n.vlbl) AND n.prop @> '{"name": "peter"}'
            ), 
            cte_1 AS (
                SELECT e.heid, e.helbl, e.prop, h.outhv AS source, unnest(h.inhv) AS target
                FROM hyperedge_attr_table AS e 
                INNER JOIN 
                hyperedge_table AS h 
                ON e.heid = h.heid 
                WHERE 'knows' ILIKE e.helbl AND e.prop @> '{"since": "2020-01-01"}'
            ),
            cte_2 AS (
                SELECT m.vid, m.vlbl, m.prop
                FROM 
                vertex_table AS m
                WHERE 'Person' ILIKE ANY(m.vlbl) AND m.prop @> '{}'
            )
            SELECT n.vid, n.vlbl, n.prop, e.heid, e.helbl, e.prop, m.vid, m.vlbl, m.prop
            FROM
            cte_0 as n
            INNER JOIN
            cte_1 AS e
            ON n.vid = e.source
            INNER JOIN 
            cte_2 AS m
            ON e.target = m.vid
        '''
        sql = sql.replace('\n', '').replace(' ', '').lower()
        expected_sql = expected_sql.replace('\n', '').replace(' ', '').lower()
        # print(sql)
        # print(expected_sql)
        self.assertEqual(sql, expected_sql)
        

if __name__ == '__main__':
    unittest.main()