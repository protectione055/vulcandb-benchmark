class  InterferenceGraph:
    def __init__(self):
        self.label_nodes = {} #label->node_id
        self.colors = []  #每个顶点的着色
        self.edges = [] #顶点邻接表
        
    def label_hash(self, label):
        """
        Get the index of the label.
        """
        return self.colors[self.label_nodes[label]]
        
    def add_node(self, label):
        """
        Add a node to the graph.
        """
        if label in self.label_nodes:
            # 如果label已经存在，返回对应的nodeid
            return self.label_nodes[label]
        node_id = len(self.label_nodes)
        self.label_nodes[label] = node_id
        self.edges.append([])
        return node_id
    
    def add_edge(self, node1, node2):
        """
        Add an edge to the graph.
        """
        if node1 < 0 or node1 >= len(self.edges):
            print(f'{node1} not in intefeence graph')
        if node2 < 0 or node2 >= len(self.edges):
            print(f'{node2} not in intefeence graph')
        self.edges[node1].append(node2)
        self.edges[node2].append(node1)
        
    def dsatur(self):
        """
        DSATUR图着色算法实现.
        首先初始化颜色、度数和饱和度的列表。然后，它选择度数最大的顶点并为其分配颜色 1。然后，它进入一个循环，直到所有的顶点都被着色。在每个循环中，它首先更新所有顶点的饱和度，然后选择饱和度最大的顶点。如果有多个顶点具有相同的饱和度，那么它选择度数最大的顶点。然后，它为选定的顶点分配一个尚未被其邻居使用的颜色。在图着色问题中，饱和度是一个顶点相邻的顶点已经被分配的颜色的数量。在 DSATUR 算法中，饱和度用于确定下一个需要着色的顶点。具体来说，DSATUR 算法会优先为饱和度最高的顶点分配颜色。如果有多个顶点的饱和度相同，那么 DSATUR 算法会选择度数最大的顶点。
        """
        def get_saturation(u):
            # 获取顶点u的饱和度
            adjacent_colors = set(self.colors[v] for v in self.edges[u] if self.colors[v] != -1)
            return len(adjacent_colors)

        def select_most_saturated_vertex():
            # 选择饱和度最高的未着色顶点
            max_saturation = -1
            selected_vertex = -1
            for u in range(len(self.label_nodes)):
                if self.colors[u] == 0:  # 未着色
                    saturation = get_saturation(u)
                    if saturation > max_saturation or (saturation == max_saturation and len(self.edges[u]) > len(self.edges[selected_vertex])):
                        max_saturation = saturation
                        selected_vertex = u
            return selected_vertex

        # 初始化
        n = len(self.label_nodes)
        self.colors = [0] * n

        # 用DSATUR算法进行图着色
        for _ in range(len(self.label_nodes)):
            u = select_most_saturated_vertex()
            if u == -1:
                break  # 所有顶点已着色

            # 找到可以用的最小颜色
            forbidden_colors = set(self.colors[v] for v in self.edges[u] if self.colors[v] != 0)
            color = 1
            while color in forbidden_colors:
                color += 1
            self.colors[u] = color

        return self.colors

    def welsh_powell(self):
        """
        Welsh-Powell算法
        """
        # 初始化
        n = len(self.edges)
        self.colors = [0] * n
        degrees = [len(adj_list) for adj_list in self.edges]
        nodes = list(range(n))
        nodes.sort(key=lambda node: degrees[node], reverse=True)
        # 着色
        color = 0
        while nodes:
            color += 1
            node = nodes.pop(0)
            self.colors[node] = color
            for i in nodes[:]:
                if not self.colors[i] and not i in self.edges[node]:
                    self.colors[i] = color
                    nodes.remove(i)
        return self.colors
        
    def graph_coloring(self, algorithm = 'DSATUR'):
        """
        Color the graph.
        """
        if algorithm == 'DSATUR':
            return self.dsatur()
        
        elif algorithm == 'Welsh-Powell':
            return self.welsh_powell()
        
    def verify_coloring(self):
        """
        Verify the graph coloring.
        """
        for i in range(len(self.edges)):
            for j in self.edges[i]:
                if self.colors[i] == self.colors[j]:
                    return False
        return True
    