import logging
import sys
from dataclasses import dataclass
from typing import Any, Generic, TypeVar
from collections.abc import Mapping

from opentelemetry import trace

from datapizza.core.models import ChainableProducer, PipelineComponent

tracer = trace.get_tracer(__name__)
log = logging.getLogger(__name__)


START = sys.intern("__start__")
END = sys.intern("__end__")


StateT = TypeVar("StateT", bound=Mapping[str, Any])


@dataclass
class Edge:
    from_node_name: str
    to_node_name: str


@dataclass
class ConditionalEdge:
    from_node_name: str
    to_node_names: list[str]
    component: PipelineComponent


@dataclass
class Node:
    component: PipelineComponent


class StateGraph(Generic[StateT]):
    """
    A pipeline that runs a graph of a dependency graph.
    """

    # TODO: async
    # TODO: accept initial state
    # TODO: allow callable

    nodes: dict[str, Node]
    edges: list[Edge | ConditionalEdge]

    state_schema: type[StateT]

    def __init__(self, state_schema: type[StateT]):
        self.nodes = {}
        self.edges = []

        self.state_schema = state_schema

    def _get_edge_from(self, node_name: str) -> Edge | ConditionalEdge:
        # TODO: replace to map and check on duplicates
        matches = [d for d in self.edges if d.from_node_name == node_name]

        if not matches:
            raise ValueError(f"No edge found from node '{node_name}'.")

        if len(matches) > 1:
            raise ValueError(f"Multiple edges found from node '{node_name}'.")

        return matches[0]

    def add_module(
        self,
        node_name: str,
        node: PipelineComponent,
    ):
        """
        Add a module to the pipeline.

        Args:
            node_name (str): The name of the module.
            node (PipelineComponent): The module to add.
        """
        node_component: PipelineComponent

        # Nodes must be ChainableProducer or PipelineComponent or callable
        if isinstance(node, ChainableProducer):
            module_component = node.as_module_component()
            node_component = module_component
        elif isinstance(node, PipelineComponent) or callable(node):
            node_component = node
        else:
            raise ValueError(
                f"Node {node_name} must be a ChainableProducer, PipelineComponent, or callable."
            )

        self.nodes[node_name] = Node(
            component=node_component,
        )

    def connect(
        self,
        source_node: str,
        target_node: str,
    ):
        """
        Connect two nodes in the pipeline.

        Args:
            source_node (str): The name of the source node.
            target_node (str): The name of the target node.
        """
        self.edges.append(
            Edge(
                from_node_name=source_node,
                to_node_name=target_node,
            )
        )

    def branch(
        self,
        node_name: str,
        node: PipelineComponent,
        path_map: list[str],
    ):
        self.edges.append(
            ConditionalEdge(
                from_node_name=node_name,
                to_node_names=path_map,
                component=node,
            )
        )

    def _evaluate_edge(self, edge: Edge | ConditionalEdge, state: StateT) -> str:
        if isinstance(edge, Edge):
            return edge.to_node_name

        elif isinstance(edge, ConditionalEdge):
            node_name = edge.component(**state)
            if node_name not in edge.to_node_names:
                raise ValueError(
                    f"Branch node returned invalid path '{node_name}', expected one of {edge.to_node_names}."
                )
            return node_name

        else:
            raise ValueError("Unknown edge type.")

    def run(self) -> StateT:
        """
        Run the pipeline.

        Returns:
            StateT: The state of the pipeline.
        """
        state = self.state_schema()

        current_edge = self._get_edge_from(START)

        while True:
            node_name = self._evaluate_edge(current_edge, state)
            if node_name == END:
                break

            node = self.nodes[node_name]

            try:
                log.debug(f"State before node {node_name}: {state}")

                state = node.component(**state)

                # Get the next edge
                current_edge = self._get_edge_from(node_name)

            except Exception as e:
                log.error(f"Error running node {node_name}: {e!s}")
                raise

        return state
