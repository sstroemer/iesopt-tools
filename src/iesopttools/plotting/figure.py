from .style import setup


class Trace:
    def __init__(self, trace_type: str, data, *, name: str | None = None, sign: float = 1.0):
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
    def __init__(self, *, backend: str = "plotly", style: str | None = None):
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
    
    def add(self, trace: Trace):
        self._traces.append(trace)
    
    def show(self):
        if self._fig is None:
            self.render()
        
        if self._backend == "plotly":
            self._fig.show()

    def render(self):
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
