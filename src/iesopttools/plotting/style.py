def setup(backend: str):
    if backend == "plotly":
        try:
            import plotly
        except ImportError:
            raise ImportError("Failed loading `plotly`; please install it using `uv add plotly`.")

        _setup_plotly_template_foo()


def _setup_plotly_template_foo():
    import plotly.graph_objects as go
    import plotly.io as pio

    pio.templates["foo"] = go.layout.Template(
        layout=dict(
            title=dict(
                font=dict(family="HelveticaNeue-CondensedBold, Helvetica, Sans-serif", size=30, color="#333"),
            ),
            font=dict(family="Helvetica Neue, Helvetica, Sans-serif", size=16, color="#333"),
            colorway=[
                "#10798E",
                "#C1DEC1",
                "#E18B1A",
                "#EA00E3",
                "#E7DA59",
                "#F1A8BD",
                "#6A7C59",
                "#8C0993",
                "#C1E0A4",
                "#8383FD",
                "#5DC885",
                "#FE39FB",
                "#66E0DD",
                "#9A54AD",
                "#2FDF3E",
                "#A945C9",
                "#85903F",
                "#530B0B",
                "#9A54AD",
                "#4FE77A",
                "#D40000",
            ],
            hovermode="x unified",
        ),
        data=dict(
            bar=[
                go.Bar(
                    texttemplate="%{value:$.2s}",
                    textposition="outside",
                    textfont=dict(family="Helvetica Neue, Helvetica, Sans-serif", size=20, color="#FFFFFF"),
                )
            ]
        ),
    )
