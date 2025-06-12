import pandas as pd
from .style import setup


class Trace:
    """
    A trace represents a single data series in a figure, such as a line or bar chart.
    It is initialized with a trace type, data, and optional parameters like name and sign.
    """
    def __init__(self, trace_type: str, data, *, name: str | None = None, sign: float = 1.0, **kwargs):
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
               
        if isinstance(data, pd.DataFrame):
            self._data = data
        else:
            # TODO: if `RDBEntryRelation` (and if has snapshots)
            self._rdbentry = data.entry
            self._data = data.to_df()
        
        self._data.set_index("snapshot", inplace=True)
        self._data["value"] *= sign
        self._name = name if name else "unknown"  # TODO
        self._type = trace_type

        if name is not None:
            self._name = name
        else:
            try:
                self._name = self._data["component"].iloc[0]
                self._name = self._name.split(".")[0]
                self._name = self._name.replace("_", " ")
            except Exception:
                self._name = "unknown"

        aggregate_into = kwargs.pop("aggregate_into", None)
        if aggregate_into:
            self._data.reset_index(inplace=True)
            self._data = self._data.groupby(self._data.index // (len(self._data) / aggregate_into)).agg({"value": "sum"})
            self._data.reset_index(inplace=True)

        self._kwargs = kwargs
    
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

        x = list(range(1, 1 + len(x)))

        if self._type == "bar":
            return go.Bar(x=x, y=y, name=self._name, marker_color=self._kwargs.get("color", None))
        elif self._type.startswith("line"):
            mode = "lines"
            line_shape = "linear"

            parts = self._type.split("+")
            if "markers" in parts:
                mode += "+markers"
            if "hv" in parts:
                line_shape = "hv"

            line = dict()
            for param in ["color", "width"]:
                if param in self._kwargs:
                    line[param] = self._kwargs[param]
            if len(line) == 0:
                line = None

            return go.Scatter(x=x, y=y, mode=mode, line_shape=line_shape, name=self._name, line=line)
        else:
            raise ValueError(f"Unsupported trace type: {self._type}.")


class Figure:
    """
    A Figure is a collection of traces that can be rendered and displayed.
    It supports different backends for rendering, such as `plotly`.
    The figure can be initialized with a specific backend and an optional style.
    """
    def __init__(self, *, backend: str = "plotly", style: str | None = None, labels: dict | None = None, skip_empty: bool = False, **kwargs):
        """
        Initializes a Figure object with a specified backend and optional style.

        :param backend: The backend to use for rendering the figure, e.g., 'plotly'.
        :type backend: str
        
        :param style: Optional style template for the figure, used with the specified backend.
        :type style: str | None
        """
        self._backend = backend
        self._labels = labels if labels else dict()
        self._style = style
        self._skip_empty = skip_empty

        self._traces = []
        self._fig = None

        self._kwargs = kwargs
        
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
        if self._skip_empty:
            if trace._data["value"].abs().sum() < 1e-6:
                return
        self._traces.append(trace)
    
    def show(self, *, xslice: tuple | None = None) -> None:
        """
        Displays the figure using the specified backend. If the figure has not been rendered yet, it will call
        `render()` first.
        """
        if self._fig is None:
            self.render(xslice = xslice)
        
        if self._backend == "plotly":
            self._fig.show()

    def render(self, *, xslice) -> None:
        """
        Renders the figure based on the traces added to it. It initializes the figure with the traces and applies any
        specified style.
        """
        if self._backend == "plotly":
            import plotly.graph_objects as go
            import plotly.io as pio

            # TODO:
            # Check if this is a single-entry figure or a entry-comparison figure.
            # entries = [trace._rdbentry for trace in self._traces]
            # x = [entry.name for entry in entries]
            # if len(set(x)) == 1:
            #     x = entries[0].snapshots

            # Add colors to traces, so that they are consistent across figures even if re-ordered.
            colorway = pio.templates[self._style if self._style else "plotly"].layout.colorway
            for (i, trace) in enumerate(self._traces):
                if "color" not in trace._kwargs:
                    trace._kwargs["color"] = colorway[i % len(colorway)]

            x = self._traces[0]._data.index
            if xslice is not None:
                x = x[xslice[0]:xslice[1]]

            if self._kwargs.get("barmode", None) in ["stack", "relative"]:
                # Get all traces that are (not) bars.
                bt = [t for t in self._traces if t._type == "bar"]
                nt = [t for t in self._traces if t._type != "bar"]

                # Keep traces with low volatility close to the x-axis.
                bt.sort(
                    key=lambda t: (
                        t._data.loc[x, "value"].std() / max(t._data.loc[x, "value"].abs().mean(), 1e-6),
                        t._data.loc[x, "value"].abs().mean()
                    )
                )

                # Add the bar traces first, then the non-bar traces.
                self._traces = bt + nt

            self._fig = go.Figure(data=[trace.get(x, backend=self._backend) for trace in self._traces])

            if self._style:
                self._fig.update_layout(template=self._style)
            
            for (k, v) in self._kwargs.items():
                self._fig.update_layout(**{k: v})

            self._fig.update_layout(
                title=self._labels.get("title", ""),
                xaxis_title=self._labels.get("x", ""),
                yaxis_title=self._labels.get("y", "")
            )
