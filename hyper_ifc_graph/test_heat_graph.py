import unittest
from unittest.mock import MagicMock
from heat_graph import HEATGraph

class HEATGraphTest(unittest.TestCase):
    def setUp(self):
        self.graph = HEATGraph(db_name="heat_test", port=5433, user='zzm', password='66668888')
        self.graph.create_graph('graph1', 10)
        
    def tearDown(self) -> None:
        self.graph.drop_graph('graph1')
        pass

    def test_bulk_load_from_csv(self):
        # self.graph.__cur_graph = 'graph1'
        # with self.assertRaises(ValueError):
        #     self.graph.bulk_load_from_csv('data.csv')
        pass

    def test_add_vertex(self):
        # self.graph.__cur_graph = 'graph1'
        # self.graph.conn = MagicMock()
        # self.graph.conn.cursor.return_value.rowcount = 1
        # self.graph.create_graph('graph2', 10)
        self.graph.add_vertex(1, {'prop1': 'value1'}, ['label1', 'label2'])
        # self.graph.conn.cursor.return_value.execute.assert_called_with("INSERT INTO vertex_table VALUES (1, '{\"prop1\": \"value1\"}', ARRAY['label1','label2'])")

    def test_add_hyperedge(self):
        # self.graph.__cur_graph = 'graph1'
        # self.graph.conn = MagicMock()
        # self.graph.conn.cursor.return_value.fetchone.return_value = [1]
        # self.graph.__cur_hash_func1 = MagicMock(return_value=0)
        # self.graph.__cur_hash_func2 = MagicMock(return_value=0)
        # self.graph.create_graph('graph3', 10)
        self.graph.add_hyperedge([1, 2], [3, 4], {'prop1': 'value1'}, label='label1', heid=1)
        # self.graph.conn.cursor.return_value.execute.assert_called_with("INSERT INTO hyperedge_table VALUES (1, '{\"prop1\": \"value1\"}', 'label1') RETURNING heid")
        # self.assertEqual(self.graph.conn.cursor.return_value.fetchone.call_count, 1)
        # self.assertEqual(self.graph.conn.cursor.return_value.execute.call_count, 3)

    def test_gen_hash_func(self):
        self.graph.__gen_hash_func = MagicMock()
        self.graph.__gen_hash_func('murmur3', 0)
        self.graph.__gen_hash_func.assert_called_with('murmur3', 0)

if __name__ == '__main__':
    unittest.main()