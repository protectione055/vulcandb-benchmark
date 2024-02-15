import unittest
from interference_graph import *

class InterferenceGraphTest(unittest.TestCase):
    def test_add_node(self):
        graph = InterferenceGraph([])
        node_id = graph.add_node('A')
        self.assertEqual(node_id, 0)
        self.assertEqual(len(graph.label_nodes), 1)
        self.assertEqual(len(graph.edges), 1)

        node_id = graph.add_node('B')
        self.assertEqual(node_id, 1)
        self.assertEqual(len(graph.label_nodes), 2)
        self.assertEqual(len(graph.edges), 2)

        node_id = graph.add_node('A')
        self.assertEqual(node_id, 0)
        self.assertEqual(len(graph.label_nodes), 2)
        self.assertEqual(len(graph.edges), 2)

    def test_add_edge(self):
        graph = InterferenceGraph([])
        graph.add_node('A')
        graph.add_node('B')
        graph.add_node('C')

        graph.add_edge(0, 1)
        self.assertEqual(len(graph.edges[0]), 1)
        self.assertEqual(len(graph.edges[1]), 1)
        self.assertEqual(graph.edges[0][0], 1)
        self.assertEqual(graph.edges[1][0], 0)

        graph.add_edge(1, 2)
        self.assertEqual(len(graph.edges[1]), 2)
        self.assertEqual(len(graph.edges[2]), 1)
        self.assertEqual(graph.edges[1][1], 2)
        self.assertEqual(graph.edges[2][0], 1)

    def test_dsatur(self):
        graph = InterferenceGraph([])
        graph.add_node('A')
        graph.add_node('B')
        graph.add_node('C')
        graph.add_node('D')
        graph.add_node('E')

        graph.add_edge(0, 1)
        graph.add_edge(0, 2)
        graph.add_edge(1, 2)
        graph.add_edge(1, 3)
        graph.add_edge(2, 3)
        graph.add_edge(2, 4)
        graph.add_edge(3, 4)
        
        print(graph.label_nodes)
        print(graph.edges)

        colors = graph.dsatur()
        assert(graph.verify_coloring())
        print(colors)

    def test_welsh_powell(self):
        graph = InterferenceGraph([])
        graph.add_node('A')
        graph.add_node('B')
        graph.add_node('C')
        graph.add_node('D')
        graph.add_node('E')

        graph.add_edge(0, 1)
        graph.add_edge(0, 2)
        graph.add_edge(1, 2)
        graph.add_edge(1, 3)
        graph.add_edge(2, 3)
        graph.add_edge(2, 4)
        graph.add_edge(3, 4)
        
        # print(graph.label_nodes)
        # print(graph.edges)

        colors = graph.welsh_powell()
        assert(graph.verify_coloring())
        
        # print(colors)

if __name__ == '__main__':
    unittest.main()