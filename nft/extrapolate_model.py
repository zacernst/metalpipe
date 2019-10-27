import networkx as nx
from nft_nodes import *


t1 = TableDataSource(config_file="actor_table.yaml")
t2 = TableDataSource(config_file="store_table.yaml")
t3 = TableDataSource(config_file="film_table.yaml")
t4 = TableDataSource(config_file="film_actor_table.yaml")
t5 = TableDataSource(config_file="film_category_table.yaml")
t6 = TableDataSource(config_file="customer_table.yaml")
t7 = TableDataSource(config_file="inventory_table.yaml")
t8 = TableDataSource(config_file="rental_table.yaml")
t9 = TableDataSource(config_file="customer_list_table.yaml")

entity_dict = {}
property_dict = {}
relationship_dict = {}


def entity_factory(entity_name):
    global entity_dict
    if entity_name in entity_dict:
        return entity_dict[entity_name]
    else:
        new_entity = Entity(name=entity_name)
        entity_dict[entity_name] = new_entity
        return new_entity


class Node:
    def __init__(self, name=None):
        self.name = name

    def __repr__(self):
        out = f"{self.__class__.__name__}: {self.name}"
        return out


class Edge:
    def __init__(self, name=None, relationship_assertion=None):
        self.name = name
        self.relationship_assertion = relationship_assertion

    def __repr__(self):
        out = f"{self.__class__.__name__}: {self.name}"
        return out


class Relationship(Edge):
    pass


class Entity(Node):
    pass


class Property(Node):
    pass


model_graph = nx.DiGraph()


for entity in is_entity_type(X0):
    entity_name = entity[0]
    entity = Entity(name=entity_name)
    model_graph.add_node(Entity(name=entity_name))
    for property_type in entity_has_property(entity_name, X0):
        name = property_type[0]
        entity_property = Property(name=name)
        model_graph.add_node(entity_property)
        model_graph.add_edge(entity, entity_property)


for relationship_assertion, source_entity, target_entity in (
    relationship_has_source_entity_type(X0, X1)
) & relationship_has_target_entity_type(X0, X2):
    source_entity = entity_factory(source_entity)
    target_entity = entity_factory(target_entity)
    model_graph.add_edge(
        source_entity, target_entity, relationship=relationship_assertion
    )

# Model graph is created
# generate walk

for node in model_graph.nodes():
    print("---", node)
    for source, target in model_graph.edges(node):
        print(node, source, target)
        if isinstance(source, (Entity,)) and isinstance(target, (Property,)):
            print(f"source: {source} has property {target}")
        elif isinstance(source, (Entity,)) and isinstance(target, (Entity,)):
            print(f"Source: {source} related to {target}")
        else:
            raise Exception("This should not happen.")


def foo():
    variable_counter = 0
    for table in is_table_data_source(X0):
        table = table[0]
        for name_assertion in is_name_assertion(X0) & assertion_in_table(
            X0, table
        ):
            name_assertion = name_assertion[0]
            name_column = assertion_has_column(name_assertion, X0)
            name_column = name_column[0][0]
            print(name_assertion, name_column)
            +table_and_name_column_has_variable(
                table, name_column, variable_counter
            )
            variable_counter += 1


class CounterDict:
    def __init__(self):
        self._dict = {}
        self._counter = 0

    def __getitem__(self, key):
        if key not in self._dict:
            self._dict[key] = self._counter
            self._counter += 1
        return self._dict[key]


class CypherStatement:
    def __init__(self, variable=None):
        self.variable = variable


class CypherVariable:
    counter_dict = CounterDict()

    def __init__(self, variable='x', number=None, parent_entity=None):
        assert parent_entity is not None, 'CypherVariable requires parent_entity to be explicitly set.'
        self.variable = variable
        self.number = number or self.counter_dict[parent_entity]

    def __repr__(self):
        return f'{self.variable}{self.number}'


class CypherNode(CypherStatement):
    def __init__(self, variable='x', number=None, entity_type=None):
        assert entity_type is not None, 'CypherNode requires entity_type to be explicitly set.'
        self.entity_type = entity_type
        self.variable = CypherVariable(variable=variable, number=number, parent_entity=self.entity_type)
        self.property_list = [entity_property[0] for entity_property in entity_has_property(self.entity_type, X0)]

    def return_properties(self):
        out = [f'{self.variable}.{property}' for property in self.property_list]
        return out

    def node_syntax(self):
        out = f'({self.variable}:{self.entity_type})'
        return out


class CypherRelationship(CypherStatement):
    def __init__(self, variable='x', number=None, relationship_type=None):
        self.variable = CypherVariable(variable=variable, number=number, parent_entity=relationship_type)
        self.relationship_type = relationship_type
        self.property_list = [
            relationship_property_name
            for _, relationship_property_name in (
                assertion_has_relationship_property_type(X0, X1) &
                (X0._relationship_type == self.relationship_type))
        ]

    def relationship_syntax(self):
        out = f'[{self.variable}:{self.relationship_type}]'
        return out

    def return_properties(self):
        out = [f'{self.variable}.{property}' for property in self.property_list]
        return out


class CypherChain(CypherStatement):
    def __init__(self, source=None, relationship=None, target=None):
        self.source = source
        self.relationship = relationship
        self.target = target

    def chain_syntax(self):
        out = (
                f'{self.source.node_syntax()}-'
                f'{self.relationship.relationship_syntax()}->'
                f'{self.target.node_syntax()}')
        return out

    def return_properties(self):
        return (
            self.source.return_properties() +
            self.relationship.return_properties() +
            self.target.return_properties()
        )


counter_dict = CounterDict()
chain_list = []
for relationship, source_entity, target_entity in (
    is_relationship_assertion(X0)
    & relationship_has_source_entity_type(X0, X1)
    & relationship_has_target_entity_type(X0, X2)
    & (X1 != X2)
):
    if relationship == "_":
        continue
    relationship_name = relationship._relationship_type
    source_entity_node = CypherNode(entity_type=source_entity)
    target_entity_node = CypherNode(entity_type=target_entity)
    relationship = CypherRelationship(relationship_type=relationship_name)
    chain = CypherChain(source=source_entity_node, relationship=relationship, target=target_entity_node)
    chain_list.append(chain)


match_clause_chains = ', '.join([chain.chain_syntax()  for chain in chain_list])
all_return_properties = list(itertools.chain(*[chain.return_properties() for chain in chain_list]))
deduped_return_properties = []
for return_property in all_return_properties:
    if return_property not in deduped_return_properties:
        deduped_return_properties.append(return_property)
return_properties_syntax = f'RETURN {", ".join(deduped_return_properties)}'
master_table_cypher_query = f'MATCH {match_clause_chains} {return_properties_syntax};'

