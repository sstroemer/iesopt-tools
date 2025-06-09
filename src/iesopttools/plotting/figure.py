from .style import setup


class Trace:
    """
    A trace represents a single data series in a figure, such as a line or bar chart.
    It is initialized with a trace type, data, and optional parameters like name and sign.
    """
    def __init__(self, trace_type: str, data, *, name: str | None = None, sign: float = 1.0):
        """
        Initializes a Trace object, that represents a single data series in a figure.

        :param trace_type: The type of the trace, e.g., 'line', 'bar', 'line+markers', etc.
        :type trace_type: str
        
        :param data: The data to be plotted, typically an RDBEntryRelation or similar structure.
        :type data: RDBEntryRelation
        
        :param name: Optional name for the trace, used for labeling in the plot.
        :type name: str | None
        
        :param sign: A multiplier for the trace values, typically used to invert the sign of the data.
        :type sign: float
        
        :raises ValueError: If `sign` is not 1.0 or -1.0.
        """
        if sign not in [1.0, -1.0]:
            raise ValueError("`sign` must be either `1.0` or `-1.0`.")
        
        # TODO: if `RDBEntryRelation` (and if has snapshots)
        self._data = data.df()
        self._data.set_index("snapshot", inplace=True)
        self._data["value"] *= sign

        self._name = name if name else "unknown"  # TODO
        self._type = trace_type
        self._rdbentry = data.entry
    
    def get(self, x = None, *, backend: str):
        """
        Returns the trace data formatted for the specified backend.

        :param x: Optional x-axis values, used to slice the data.
        :type x: list | None
        :param backend: The backend to use for rendering the trace, e.g., 'plotly'.
        :type backend: str
        
        :return: A trace object compatible with the specified backend.
        """
        y = self._data["value"].values if x is None else self._data.loc[x, "value"].values
        
        if backend == "plotly":
            return self._get_plotly(x, y)
        else:
            raise ValueError(f"Unsupported backend: {backend}. Supported backends are: 'plotly'.")
    
    def _get_plotly(self, x, y):
        import plotly.graph_objects as go
        if self._type == "bar":
            return go.Bar(x=x, y=y, name=self._name)
        elif self._type.startswith("line"):
            mode = "lines"
            line_shape = "linear"

            parts = self._type.split("+")
            if "markers" in parts:
                mode += "+markers"
            if "hv" in parts:
                line_shape = "hv"

            return go.Scatter(x=x, y=y, mode=mode, line_shape=line_shape, name=self._name)
        else:
            raise ValueError(f"Unsupported trace type: {self._type}.")


class Figure:
    """
    A Figure is a collection of traces that can be rendered and displayed.
    It supports different backends for rendering, such as `plotly`.
    The figure can be initialized with a specific backend and an optional style.
    """
    def __init__(self, *, backend: str = "plotly", style: str | None = None):
        """
        Initializes a Figure object with a specified backend and optional style.

        :param backend: The backend to use for rendering the figure, e.g., 'plotly'.
        :type backend: str
        
        :param style: Optional style template for the figure, used with the specified backend.
        :type style: str | None
        """
        self._backend = backend
        self._style = style

        self._traces = []
        self._fig = None
        
        if self._backend == "plotly":
            try:
                import plotly
            except ImportError:
                raise ImportError("Failed loading `plotly`; please install it using `uv add plotly`.")
        
        setup(self._backend)
    
    def add(self, trace: Trace) -> None:
        """
        Adds a trace to the figure.

        :param trace: A Trace object to be added to the figure.
        :type trace: Trace
        """
        self._traces.append(trace)
    
    def show(self) -> None:
        """
        Displays the figure using the specified backend. If the figure has not been rendered yet, it will call
        `render()` first.
        """
        if self._fig is None:
            self.render()
        
        if self._backend == "plotly":
            self._fig.show()

    def render(self) -> None:
        """
        Renders the figure based on the traces added to it. It initializes the figure with the traces and applies any
        specified style.
        """
        if self._backend == "plotly":
            import plotly.graph_objects as go

            # Check if this is a single-entry figure or a entry-comparison figure.
            entries = [trace._rdbentry for trace in self._traces]
            x = [entry.name for entry in entries]
            if len(set(x)) == 1:
                x = entries[0].snapshots

            self._fig = go.Figure(data=[trace.get(x, backend=self._backend) for trace in self._traces])

            if self._style:
                self._fig.update_layout(template=self._style)
