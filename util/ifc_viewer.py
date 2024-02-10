########################################################
# This script is used to view the structure of ifc file.
# 
# Usage:
#   python3 ifc_viewer.py
#   load <ifc_file_path> - load ifc file
#   show <guid> - show entity tree
#   show #<id> - show entity tree
#   set_filter <entity_name> - filter entity
#   set_query <entity_name> - query entity
#   unset_filter <entity_name> - unset filter
#   unset_query <entity_name> - unset query
#   help - show help
#   exit/q - exit program
########################################################


import ifcopenshell
import re

def print_help():
    print("load <ifc_file_path> - load ifc file")
    print("show <guid> - show entity tree")
    print("show #<id> - show entity tree")
    print("set_filter <entity_name> - filter entity")
    print("set_query <entity_name> - query entity")
    print("unset_filter <entity_name> - unset filter")
    print("unset_query <entity_name> - unset query")
    print("exit - exit program")
    print("q - exit program")

def show_entity_tree(entity, indent='', filter=[], query=[]):
    if indent != '':
        print(indent + '|--' + str(entity))
    else:
        print(str(entity))
    for attr in entity:
        if type(attr) is ifcopenshell.entity_instance and (attr.is_a().lower() in filter or len(query) > 0 and attr.is_a().lower() not in query):
            continue
        if type(attr) is ifcopenshell.entity_instance:
            show_entity_tree(attr, indent + '\t')
        elif type(attr) is tuple and len(attr) > 0 and type(attr[0]) is ifcopenshell.entity_instance:
            for sub_entity in attr:
                show_entity_tree(sub_entity, indent+'\t')

if __name__ == '__main__':
    model = None
    pattern = re.compile(r'^(load|show|exit|q|set_filter|set_query|unset_filter|unset_query|help)( (.+))?', re.IGNORECASE)
    filter = [x.lower() for x in ['IfcOwnerHistory']]
    query = []
    while(True):
        try:
            command = input("\033[32mifc@viewer->\033[0m ")
            match = pattern.match(command)
            if match is None:
                if command == '\n':
                    pass
                else:
                    print("Invalid command.")
                continue
            opt = match.group(1)
            arg = match.group(3)
            
            if opt.lower() == 'load':
                model = ifcopenshell.open(arg)
            elif opt.lower() == 'show':
                entity = None
                if arg.startswith('#'):
                    entity = model.by_id(int(arg[1:]))
                else:
                    entity = model.by_guid(arg)
                show_entity_tree(entity, filter=filter, query=query)
            elif opt.lower() == 'set_filter':
                filter.append(arg.lower())
            elif opt.lower() == 'set_query':
                query.append(arg.lower())
            elif opt.lower() == 'unset_filter':
                filter.remove(arg.lower())
            elif opt.lower() == 'unset_query':
                query.remove(arg.lower())
            elif opt.lower() == 'help':
                print_help()
            elif opt.lower() == 'exit' or opt.lower() == 'q':
                break
        except KeyboardInterrupt:
            exit(0)
        except Exception as e:
            print(e)