from uuid import uuid4
from neo4j import GraphDatabase
from py2neo import Graph
import ifcopenshell
import os

from util.common import Timer


class Node(dict):
    """
    A node in a graph.
    """

    __primarylabel__ = None
    __primarykey__ = None

    def __init__(self, *args, **kwargs):
        """
        Create a new node.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.labels = set()

    def add_label(self, label):
        """
        Add a label to the node.

        Args:
            label (str): The label to add.
        """
        self.labels.add(label)
        
    def format_labels(self):
        """
        Get the labels of the node.

        Returns:
            set: The labels of the node.
        """
        res = res = self.__primarylabel__
        for label in self.labels:
            if label != self.__primarylabel__:
                res += f":{label}"
        return res

    def __repr__(self):
        """
        Get the string representation of the node.

        Returns:
            str: The string representation of the node.
        """
        return str(self)

    def __str__(self):
        """
        Get the string representation of the node.

        Returns:
            str: The string representation of the node.
        """
        if len(self.items()) == 0:
            return "{}"

        res = "{"
        for key, value in self.items():
            if isinstance(value, (int, float)):
                res += f"{key}: {value}, "
            elif isinstance(value, str):
                value = value.replace('"', '\\"')
                res += f'{key}: "{value}", '
            else:
                res += f'{key}: "{value}", '
        res = res[:-2]
        res += "}"
        return res

    def __hash__(self):
        return hash(self.__primarykey__)

    def __eq__(self, o: object):
        if isinstance(o, Node):
            return self.__hash__() == o.__hash__()
        return False


class Relationship:
    """
    A relationship between two nodes.
    """

    def __init__(self, start_node, rel_type, end_node):
        """
        Create a new relationship.

        Args:
            start_node (Node): The start node.
            rel_type (str): The relationship type.
            end_node (Node): The end node.
        """
        self.start_node = start_node
        self.rel_type = rel_type
        self.end_node = end_node

    def __repr__(self):
        """
        Get the string representation of the relationship.

        Returns:
            str: The string representation of the relationship.
        """
        return str(self)

    def __str__(self):
        """
        Get the string representation of the relationship.

        Returns:
            str: The string representation of the relationship.
        """
        return f"Relationship({self.start_node}, {self.rel_type}, {self.end_node})"

    def __hash__(self):
        return hash((self.start_node, self.rel_type, self.end_node))

    def __eq__(self, o: object):
        if isinstance(o, Relationship):
            return self.__hash__() == o.__hash__()
        return False


class PropertyGraph:
    """
    A graph.
    """

    def __init__(self):
        """
        Create a new graph.
        """
        self.nodes = set()
        self.relationships = set()

    def merge(self, entity):
        """
        Merge an entity into the graph.

        Args:
            entity (Node or Relationship): The entity to merge.
        """
        if isinstance(entity, Node):
            self.nodes.add(entity)
        elif isinstance(entity, Relationship):
            self.relationships.add(entity)
        else:
            raise TypeError("Can only merge nodes and relationships")

    def __repr__(self):
        """
        Get the string representation of the graph.

        Returns:
            str: The string representation of the graph.
        """
        return str(self)

    def __str__(self):
        """
        Get the string representation of the graph.

        Returns:
            str: The string representation of the graph.
        """
        return f"Graph({super().__repr__()})"


# Create the basic node with literal attributes and the class hierarchy
def create_pure_node_from_ifc_entity(ifc_entity, ifc_file, hierarchy=True):
    """
    Create a pure node from an IFC entity.

    Args:
        ifc_entity (IFCEntity): The IFC entity to create the node from.
        ifc_file (IFCFile): The IFC file containing the entity.
        hierarchy (bool, optional): Flag indicating whether to include hierarchy labels.
            Defaults to True.

    Returns:
        Node: The created pure node.
    """
    node = Node()
    if ifc_entity.id() != 0:
        node["id"] = str(ifc_entity.id())
    else:
        node["id"] = str(uuid4())
    node["ifc_name"] = ifc_entity.is_a()
    if hierarchy:
        for label in ifc_file.wrapped_data.types_with_super(): 
            if ifc_entity.is_a(label):
                node.add_label(label)
    else:
        node.add_label(ifc_entity.is_a())
    attributes_type = ["ENTITY INSTANCE", "AGGREGATE OF ENTITY INSTANCE", "DERIVED"]
    for i in range(ifc_entity.__len__()):
        if not ifc_entity.wrapped_data.get_argument_type(i) in attributes_type:
            name = ifc_entity.wrapped_data.get_argument_name(i)
            name_value = ifc_entity.wrapped_data.get_argument(i)
            node[name] = name_value
    node.__primarylabel__ = ifc_entity.is_a()
    node.__primarykey__ = node["id"]
    return node


# Process literal attributes, entity attributes, and relationship attributes
def create_graph_from_ifc_entity_all(graph, ifc_entity, ifc_file):
    """
    Create a graph representation of the given IFC entity and its related entities.

    Args:
        graph: The graph object to store the entities and relationships.
        ifc_entity: The IFC entity to create the graph from.
        ifc_file: The IFC file containing the entity.

    Returns:
        None
    """
    node = create_pure_node_from_ifc_entity(ifc_entity, ifc_file)
    graph.merge(node)
    for i in range(ifc_entity.__len__()):
        if ifc_entity[i]:
            if ifc_entity.wrapped_data.get_argument_type(i) == "ENTITY INSTANCE":
                if (
                    ifc_entity[i].is_a() in ["IfcOwnerHistory"]
                    and ifc_entity.is_a() != "IfcProject"
                ):
                    continue
                sub_node = create_pure_node_from_ifc_entity(ifc_entity[i], ifc_file)
                rel = Relationship(
                    node, ifc_entity.wrapped_data.get_argument_name(i), sub_node
                )
                graph.merge(rel)
            elif (
                ifc_entity.wrapped_data.get_argument_type(i)
                == "AGGREGATE OF ENTITY INSTANCE"
            ):
                for sub_entity in ifc_entity[i]:
                    sub_node = create_pure_node_from_ifc_entity(sub_entity, ifc_file)
                    rel = Relationship(
                        node, ifc_entity.wrapped_data.get_argument_name(i), sub_node
                    )
                    graph.merge(rel)
    for rel_name in ifc_entity.wrapped_data.get_inverse_attribute_names():
        if ifc_entity.wrapped_data.get_inverse(rel_name):
            inverse_relations = ifc_entity.wrapped_data.get_inverse(rel_name)
            for wrapped_rel_entity in inverse_relations:
                rel_entity = ifc_file.by_id(wrapped_rel_entity.id())
                sub_node = create_pure_node_from_ifc_entity(rel_entity, ifc_file)
                rel = Relationship(node, rel_name, sub_node)
                graph.merge(rel)


def create_full_graph(graph, ifc_file):
    """
    Create a full graph from an IFC file.

    Args:
        graph (Graph): The graph object to store the entities.
        ifc_file (IFCFile): The IFC file object.

    Returns:
        None
    """
    idx = 1
    length = len(ifc_file.wrapped_data.entity_names())
    for entity_id in ifc_file.wrapped_data.entity_names():
        entity = ifc_file.by_id(entity_id)
        # print(idx, "/", length, entity)
        create_graph_from_ifc_entity_all(graph, entity, ifc_file)
        idx += 1


def write_graph_to_neo4j(graph, driver, database="neo4j"):
    """
    Write the graph to Neo4j.

    Args:
        graph (Graph): The graph object to store the entities.
        driver (neo4j.Driver): The Neo4j driver.

    Returns:
        None
    """
    try:
        tx = driver.begin()
        import time
        time_start = time.time()
        query = ""
        for node in graph.nodes:
            query = f"""CREATE (n: {node.format_labels()} {node});"""
            tx.run(query)
            print(query)
        time_end = time.time()
        print("Create nodes time: ", time_end - time_start)

        time_start = time.time()
        query = ""
        for rel in graph.relationships:
            start_node = rel.start_node
            end_node = rel.end_node
            query = f"""
            MATCH (parent: {start_node['ifc_name']} {{id: \"{start_node['id']}\"}}),
            (child:{end_node['ifc_name']} {{id: \"{end_node['id']}\"}})
                        CREATE (parent)-[:{rel.rel_type}]->(child);
            """
            tx.run(query)
            print(query)
        time_end = time.time()
        print("Create relationships time: ", time_end - time_start)
        
        tx.commit()
    except Exception as e:
        print("write_graph_to_neo4j: %s" % e)
        tx.rollback()
        exit(1)


class Neo4jTask3Impl:
    
    def __init__(self, args) -> None:
        self._user = args["user"]
        self._password = args["password"]
        self._host = args["host"]
        self._port = args["port"]
        self._database_name = []
        self._driver = None
    
    @Timer.eclapse
    def prepare_data(self, workloads):
        try:
            # Connect to Neo4j
            uri=f"""bolt://{self._host}:{self._port}"""
            auth = (self._user, self._password)
            self._driver = Graph(uri=uri, auth=auth)
            
            for workload in workloads:
                self._database_name.append(os.path.basename(workload).split(".")[0])
                ifc_file = ifcopenshell.open(workload)
                # Create graph
                graph = PropertyGraph()
                create_full_graph(graph, ifc_file)
                print("Graph created. Writing to Neo4j...")
                write_graph_to_neo4j(graph, self._driver)
        except Exception as e:
            print(e)
            exit(1)
    
    @Timer.eclapse
    def run(self):
        pass
    
    def cleanup(self):
        query = "MATCH (n) DETACH DELETE n"
        self._driver.execute_query(query)
        self._driver.close()
        
        