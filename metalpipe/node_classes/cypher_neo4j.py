from neo4j import GraphDatabase
from metalpipe.node import MetalNode, NothingToSeeHere
import logging

logging.basicConfig(level=logging.INFO)


class Neo4JExecutor(MetalNode):
    def __init__(
        self, uri="bolt://localhost:7687", username=None, password=None, **kwargs
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.driver = GraphDatabase.driver(
            self.uri, auth=(self.username, self.password), encrypted=False
        )
        self.session = self.driver.session()
        self.transaction = self.session.begin_transaction()
        super(Neo4JExecutor, self).__init__(**kwargs)

    def process_item(self):
        cypher = self.__message__["cypher"]
        cypher_query = cypher["cypher_query"]
        logging.info(">>>>" + str(cypher_query))
        cypher_query_parameters = cypher.get("cypher_query_parameters", {})
        result = self.transaction.run(cypher_query, **cypher_query_parameters)
        self.transaction.commit()
        self.transaction = self.session.begin_transaction()
        # self.session.sync()
        yield NothingToSeeHere()
