from lark import Lark, Transformer, v_args

class CypherTransformer(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.__vertex = []
        self.__edge = []
        
    def start(self, args):
        return args[0]
    
    def match_clause(self, args):
        return args[0]
    
    def pattern_clause(self, args):
        return args[0]
    
    def single_node_pattern(self, args):
        return (self.__vertex, self.__edge)
    
    def path_pattern(self, args):
        return (self.__vertex, self.__edge)
    
    def vertex_pattern(self, args):
        prop_filter = str(args[2]) if len(args) == 3 else '{}'
        self.__vertex.append((str(args[0]), str(args[1]), prop_filter))
        return None
    
    def edge_pattern(self, args):
        prop_filter = str(args[2]) if len(args) == 3 else '{}'
        self.__edge.append((str(args[0]), str(args[1]), prop_filter))
        return None
        
    def iterate_vertex(self):
        yield from self.__vertex
        
    def iterate_edge(self):
        yield from self.__edge

class CypherParser:
    def __init__(self) -> None:
        self.__cypher_grammar = """
            start: match_clause
            match_clause: "MATCH" pattern_clause
            pattern_clause: single_node_pattern | path_pattern
            single_node_pattern: vertex_pattern

            path_pattern: vertex_pattern ("-" edge_pattern "->" vertex_pattern)+
            edge_pattern: "[" VAR ":" LABEL JSON "]" | "[" VAR ":" LABEL "]"

            vertex_pattern: "(" VAR ":" LABEL JSON ")" | "(" VAR ":" LABEL ")"
            VAR: /[a-zA-Z_][a-zA-Z0-9_]*/
            LABEL: /[a-zA-Z_][a-zA-Z0-9_]*/
            JSON : /{[^{}]*}/
            %import common.WS
            %ignore WS
        """
        self.__parser = Lark(self.__cypher_grammar, parser='lalr', transformer=CypherTransformer())

    def parse(self, query: str) -> str:
        return self.__parser.parse(query)