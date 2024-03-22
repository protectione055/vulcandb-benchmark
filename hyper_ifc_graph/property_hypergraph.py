

class HyperVertex:
    def __init__(self, vertex):
        self.id = -1
        self.labels = []
        self.properties = None
    
    def set_id(self, id):
        self.id = id

    # 接受一个字典作为属性
    def set_property(self, property):
        self.properties = property

    
    def append_label(self, label):
        self.labels.append(label)
        
    def get_id(self):
        return self.id
    
    def get_property(self):
        return self.properties
    
    def get_labels(self):
        return self.labels
        
class HyperEdge:
    def __init__(self, hyperedge):
        self.start_vertexs = []
        self.end_vertexs = []
        self.labels = []
        self.properties = []
        
    def add_start_vertexs(self, vertexs = []):
        for v in vertexs:
            self.start_vertexs.append(v)
    
    def add_end_vertexs(self, vertexs = []):
        for v in vertexs:
            self.end_vertexs.append(v)
    
    def get_start_vertexs(self):
        return self.start_vertexs
    
    def get_end_vertexs(self):
        return self.end_vertexs

    def add_property(self, property):
        self.properties.append(property)

    def add_label(self, label):
        self.labels.append(label)

class HyperGraph:
    def __init__(self):
        self.vertexs = []
        self.hyperedges = []
    
    def add_vertex(self, vertex):
        # 判断是否有重复
        if vertex.id != -1:
            for v in self.vertexs:
                if v.id == vertex.id:
                    print('Vertex id is duplicated')
                    return -1
        vertex.set_id(len(self.vertexs))
        self.vertexs.append(vertex)
        return vertex.id
    
    def add_hyperedge(self, hyperedge):
        start_vertexs = hyperedge.get_start_vertexs()
        for v in start_vertexs:
            if v.id < 0 or v.id >= len(self.vertexs) or self.vertexs[v.id] != v:
                print('Vertex id is out of range')
                exit(1)
        end_vertexs = hyperedge.get_end_vertexs()
        for v in end_vertexs:
            if v.id < 0 or v.id >= len(self.vertexs) or self.vertexs[v.id] != v:
                print('Vertex id is out of range')
                exit(1)
        self.hyperedges.append(hyperedge)
    
    def get_vertexs(self, id):
        if id < 0 or id >= len(self.vertexs):
            print('Vertex id is out of range')
            return None
        return self.vertexs[id]