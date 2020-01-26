"""Objects tied to graph"""

from attr import attrs, attrib

from .ffunction import FFunction


def _create_inputs(inputs, function, table_keys, parameters):
    def swap_args(arg, table_keys, parameters):
        if arg in table_keys.keys():
            return table_keys[arg]
        if arg in parameters.keys():
            return arg
        raise Exception()

    return (function, *tuple(swap_args(arg, table_keys, parameters) for arg in inputs))


def _create_output(outputs, function, table_keys):
    if len(outputs) > 1:
        out = []
        for output in outputs:
            out_tuple = (output, function.name)
            out.append(out_tuple)
            table_keys[output] = out_tuple
        return tuple(out), table_keys
    out_tuple = (outputs[0], function.name)
    table_keys[outputs[0]] = out_tuple
    return out_tuple, table_keys


def _build_graph_get_parameters(steps, *tables):
    table_keys = {table: table for table in tables}
    parameters = {}
    graph = {}
    for step in steps:
        if not isinstance(step, FFunction):
            raise TypeError(f"Each item in steps must be a FFunction")
        params = step.input_parameters
        if params != {}:
            parameters.update({v.name: v for v in params.values()})
        outputs, function, inputs = step.get_step_items()
        inputs = _create_inputs(inputs, function, table_keys, parameters)
        outputs, table_keys = _create_output(outputs, function, table_keys)
        graph.update({outputs: inputs})
    graph = {
        **{table: None for table in tables},
        **{parameter: None for parameter in parameters},
        **graph,
    }
    return Graph(graph, steps, table_keys), parameters


@attrs(slots=True)
class Graph:
    """Instructions"""

    graph = attrib()
    steps = attrib()
    keys = attrib()

    def update(self, **kwargs):
        """Update graph"""
        for k, v in kwargs.items():
            self.graph[k] = v
