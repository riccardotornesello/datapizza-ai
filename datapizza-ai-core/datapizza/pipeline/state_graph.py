import logging
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

from opentelemetry import trace


tracer = trace.get_tracer(__name__)
log = logging.getLogger(__name__)


START = sys.intern("__start__")
END = sys.intern("__end__")


StateT = TypeVar("StateT", bound=dict)


@dataclass
class Edge:
    from_node_name: str
    to_node_name: str
    src_key: str | None
    dst_key: str


class StateComponent(Generic[StateT], ABC):
    def __call__(self, state: StateT) -> StateT:
        return self.run(state)

    def validate_input(self, state: StateT):
        """
        Validate the input of the component.
        """
        assert 1 == 1

    def validate_output(self, state: StateT):
        """
        Validate the output of the component.
        """
        assert 1 == 1

    def run(self, state: StateT) -> StateT:
        """
        Synchronous execution wrapper around _run with tracing and validation.

        This method is called when the component is executed in a sync context.
        """
        with tracer.start_as_current_span(f"StateComponent.{self.__class__.__name__}"):
            self.validate_input(state)
            data = self._run(state)
            self.validate_output(data)
            return data

    @abstractmethod
    def _run(self, state: StateT) -> StateT:
        """
        The core processing logic of the component.

        Subclasses must implement this method to define their specific behavior.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement the _run method"
        )


class DagPipeline(Generic[StateT]):
    """
    A pipeline that runs a graph of a dependency graph.
    """

    nodes: dict[str, StateComponent[StateT]]
    edges: list[Edge]

    state_schema: type[StateT]

    def __init__(self, state_schema: type[StateT]):
        self.nodes = {}
        self.edges = []

        self.state_schema = state_schema

    # get the nodes that depend on the given node
    def _get_edges_from(self, node_name: str) -> list[Edge]:
        return [d for d in self.edges if d.from_node_name == node_name]

    # # get the nodes that the given node depends on
    # def _get_edges_to(self, node_name: str) -> list[Edge]:
    #     return [d for d in self.edges if d.to_node_name == node_name]

    def add_module(self, node_name: str, node: StateComponent[StateT]):
        """
        Add a module to the pipeline.

        Args:
            node_name (str): The name of the module.
            node (StateComponent): The module to add.
        """
        # TODO: prevent duplicate names?

        # Nodes must be StateComponent or callable
        if isinstance(node, StateComponent) or callable(node):
            self.nodes[node_name] = node

        else:
            raise ValueError(f"Node {node_name} must be a StateComponent or callable.")

    def connect(
        self,
        source_node: str,
        target_node: str,
        target_key: str,
        source_key: str | None = None,
    ):
        """
        Connect two nodes in the pipeline.

        Args:
            source_node (str): The name of the source node.
            target_node (str): The name of the target node.
            target_key (str): The key to store the result of the target node in the source node.
            source_key (str, optional): The key to retrieve the result of the source node from the target node. Defaults to None.
        """
        self.edges.append(
            Edge(
                from_node_name=source_node,
                to_node_name=target_node,
                src_key=source_key,
                dst_key=target_key,
            )
        )

    def run(self) -> StateT:
        """
        Run the pipeline.

        Returns:
            StateT: The state of the pipeline.
        """
        state = self.state_schema()

        first_node = self._get_edges_from(START)
        if not first_node:
            raise ValueError("No starting node found in the pipeline.")
        if len(first_node) > 1:
            raise ValueError("Multiple starting nodes found in the pipeline.")

        node_name = first_node[0].to_node_name
        while node_name != END:
            node = self.nodes[node_name]
            try:
                log.debug(f"Current state: {state}")
                state = node(state)

                # Get the next node
                edges_from = self._get_edges_from(node_name)
                if not edges_from:
                    raise ValueError(f"No outgoing edges from node '{node_name}'.")
                if len(edges_from) > 1:
                    raise ValueError(
                        f"Multiple outgoing edges from node '{node_name}'."
                    )

                node_name = edges_from[0].to_node_name

            except Exception as e:
                log.error(f"Error running node {node_name}: {e!s}")
                raise

        return state
