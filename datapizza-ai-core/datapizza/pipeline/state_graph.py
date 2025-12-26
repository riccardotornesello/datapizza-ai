import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from opentelemetry import trace

from datapizza.core.models import ChainableProducer, PipelineComponent

tracer = trace.get_tracer(__name__)
log = logging.getLogger(__name__)


START = sys.intern("__start__")
END = sys.intern("__end__")


class StateComponentWrapper:
    def __init__(
        self,
        component: PipelineComponent
        | Callable[..., str]
        | Callable[..., Awaitable[Any]],
        default_data: dict[str, Any] | None = None,
        state_input_remap: dict[str, str] | None = None,
        output_key: str | None = None,
    ):
        self.component = component
        self.default_data = default_data or {}
        self.state_input_remap = state_input_remap
        self.output_key = output_key

    def __call__(self, **kwargs) -> Any:
        # Check if we are in an async context
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            return self.a_run(kwargs)
        else:
            return self.run(kwargs)

    def run(self, state: dict[str, Any]) -> Any:
        input_data = self._get_input(state)

        # If is awaitable, run in event loop
        if asyncio.iscoroutinefunction(self.component):
            output_data = asyncio.run(self.component(**input_data))
        else:
            output_data = self.component(**input_data)

        return self._get_output(output_data, state)

    async def a_run(self, state: dict[str, Any]) -> Any:
        input_data = self._get_input(state)

        if isinstance(self.component, PipelineComponent):
            output_data = await self.component.a_run(**input_data)
        elif asyncio.iscoroutinefunction(self.component):
            output_data = await self.component(**input_data)
        else:
            output_data = self.component(**input_data)

        return self._get_output(output_data, state)

    def _get_input(self, state: dict[str, Any]) -> dict[str, Any]:
        input_state = self.default_data.copy()

        if self.state_input_remap is None:
            for k, v in state.items():
                input_state[k] = v
        else:
            for k, v in state.items():
                remapped_key = self.state_input_remap.get(k)
                if remapped_key:
                    input_state[remapped_key] = v

        return input_state

    def _get_output(self, result: Any, state: dict[str, Any]) -> dict[str, Any]:
        if self.output_key:
            output_state = state.copy()
            output_state[self.output_key] = result
            return output_state
        else:
            return result


@dataclass
class Node:
    component: StateComponentWrapper


@dataclass
class SimpleEdge:
    to_node_name: str


@dataclass
class ConditionalEdge:
    to_node_names: list[str]
    component: StateComponentWrapper


StateT = TypeVar("StateT", bound=Mapping[str, Any])
Edge = SimpleEdge | ConditionalEdge


class StateGraph(Generic[StateT]):
    """
    A pipeline that runs a graph of a dependency graph.
    """

    # TODO: from yaml

    nodes: dict[str, Node]
    edges: dict[str, Edge]

    state_schema: type[StateT]

    def __init__(self, state_schema: type[StateT]):
        self.nodes = {}
        self.edges = {}

        self.state_schema = state_schema

    def _validate_new_edge(self, source_node: str, target_nodes: list[str]):
        """
        Validates a new edge.

        Args:
            source_node (str): The source node.
            target_nodes (list[str]): The target nodes.

        Raises:
            ValueError: If the edge is invalid.
        """
        if source_node not in self.nodes and source_node not in (START, END):
            raise ValueError(f"Source node {source_node} does not exist in the graph.")

        for target_node in target_nodes:
            if target_node not in self.nodes and target_node not in (START, END):
                raise ValueError(
                    f"Target node {target_node} does not exist in the graph."
                )

        if source_node in self.edges:
            raise ValueError(f"Source node {source_node} already has an outgoing edge.")

    def _validate_graph(self):
        """
        Validates the graph.

        The following conditions must be met:
        - START and END nodes must be connected

        Raises:
            ValueError: If the graph is invalid.
        """
        if START not in self.edges:
            raise ValueError("Graph must have a START node connected.")

        connected_edges = set()
        for edge in self.edges.values():
            if isinstance(edge, SimpleEdge):
                connected_edges.add(edge.to_node_name)
            elif isinstance(edge, ConditionalEdge):
                connected_edges.update(edge.to_node_names)

        if END not in connected_edges:
            raise ValueError("Graph must have an END node connected.")

    def add_module(
        self,
        node_name: str,
        node: StateComponentWrapper | PipelineComponent,
    ):
        """
        Add a module to the pipeline.

        Args:
            node_name (str): The name of the module.
            node (StateComponentWrapper | PipelineComponent): The module to add.
        """
        if node_name in self.nodes:
            raise ValueError(f"Node {node_name} already exists in the graph.")

        # Nodes must be PipelineComponent or callable
        node_component: StateComponentWrapper
        if isinstance(node, ChainableProducer):
            module_component = node.as_module_component()
            node_component = StateComponentWrapper(module_component)
        elif isinstance(node, StateComponentWrapper):
            node_component = node
        elif isinstance(node, PipelineComponent) or callable(node):
            node_component = StateComponentWrapper(node)
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
        self._validate_new_edge(source_node, [target_node])

        self.edges[source_node] = SimpleEdge(
            to_node_name=target_node,
        )

    def branch(
        self,
        node_name: str,
        node: StateComponentWrapper | PipelineComponent,
        path_map: list[str],
    ):
        self._validate_new_edge(node_name, path_map)

        node_component: StateComponentWrapper
        if isinstance(node, StateComponentWrapper):
            node_component = node
        elif isinstance(node, PipelineComponent) or callable(node):
            node_component = StateComponentWrapper(node)
        else:
            raise ValueError(
                f"Node {node_name} must be a StateComponentWrapper, PipelineComponent, or callable."
            )

        self.edges[node_name] = ConditionalEdge(
            to_node_names=path_map,
            component=node_component,
        )

    def run(self, initial_state: StateT | None = None) -> StateT:
        """
        Run the pipeline.

        Args:
            initial_state (StateT | None): The initial state of the pipeline. If None, an empty state will be used.

        Returns:
            StateT: The state of the pipeline.
        """
        self._validate_graph()

        state = self.state_schema() if initial_state is None else initial_state

        current_edge = self.edges[START]

        while True:
            # Evaluate the current edge to get the next node
            node_name = None
            if isinstance(current_edge, SimpleEdge):
                node_name = current_edge.to_node_name
            elif isinstance(current_edge, ConditionalEdge):
                node_name = current_edge.component(**state)
                if node_name not in current_edge.to_node_names:
                    raise ValueError(
                        f"Branch node returned invalid path '{node_name}', expected one of {current_edge.to_node_names}."
                    )
            else:
                raise ValueError("Unknown edge type.")

            # Check for end node
            if node_name == END:
                break

            # Execute the node
            node = self.nodes[node_name]
            try:
                log.debug(f"State before node {node_name}: {state}")

                state = node.component(**state)

                # Get the next edge
                current_edge = self.edges[node_name]

            except Exception as e:
                log.error(f"Error running node {node_name}: {e!s}")
                raise

        return state

    async def a_run(self, initial_state: StateT | None = None) -> StateT:
        """
        Run the pipeline asynchronously.

        Args:
            initial_state (StateT | None): The initial state of the pipeline. If None, an empty state will be used.

        Returns:
            StateT: The state of the pipeline.
        """
        self._validate_graph()

        state = self.state_schema() if initial_state is None else initial_state

        current_edge = self.edges[START]

        while True:
            # Evaluate the current edge to get the next node
            node_name = None
            if isinstance(current_edge, SimpleEdge):
                node_name = current_edge.to_node_name
            elif isinstance(current_edge, ConditionalEdge):
                if isinstance(current_edge.component, PipelineComponent):
                    node_name = await current_edge.component(**state)
                else:
                    node_name = current_edge.component(**state)
                if node_name not in current_edge.to_node_names:
                    raise ValueError(
                        f"Branch node returned invalid path '{node_name}', expected one of {current_edge.to_node_names}."
                    )
            else:
                raise ValueError("Unknown edge type.")

            # Check for end node
            if node_name == END:
                break

            # Execute the node
            node = self.nodes[node_name]
            try:
                log.debug(f"State before node {node_name}: {state}")

                state = await node.component(**state)

                # Get the next edge
                current_edge = self.edges[node_name]

            except Exception as e:
                log.error(f"Error running node {node_name}: {e!s}")
                raise

        return state
