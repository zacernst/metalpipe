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


entity_graph = {}
for entity in is_entity_type(X0):
    entity_name = entity[0]
    entity = Entity(name=entity_name)
    model_graph.add_node(Entity(name=entity_name), metatype="Entity")
    for property_type in entity_has_property(entity_name, X0):
        name = property_type[0]
        entity_property = Property(name=name)
        model_graph.add_node(entity_property, metatype="Property")
        model_graph.add_edge(entity, entity_property, metatype='HasProperty')


for relationship_assertion, source_entity, target_entity in (
    relationship_has_source_entity_type(X0, X1)
) & relationship_has_target_entity_type(X0, X2):
    #relationship_name = relationship.__class__.__name__
    model_graph.add_edge(
        source_entity, target_entity, relationship=relationship_assertion, metatype='Relationship'
    )
