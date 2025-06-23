import drawpyo


COLORS = {
    "electricity": "#4c00ff",
    "heat": "#7a1800",
    "hydrogen": "#00a2ff",
    "co2": "#666666",
    "gas": "#3a2f00",
}
COLORS["h2"] = COLORS["hydrogen"]
COLORS["ch4"] = COLORS["gas"]


def connect(source, target, animate=False):
    entryX = exitX = entryY = exitY = None
    strokeColor = None

    if source.cctype == "Unit":
        exitX = 1.0
        exitY = 0.5
        strokeColor = target.obj.strokeColor
    elif target.cctype == "Unit":
        entryX = 0.0
        entryY = 0.5
        strokeColor = source.obj.strokeColor
    else:
        strokeColor = target.obj.strokeColor

    drawpyo.diagram.Edge(
        page=source.obj.page,
        source=source.obj,
        target=target.obj,
        label_offset=0,
        jetty_size=40,
        rounded=True,
        jumpStyle="gap",
        flowAnimation=animate,
        pattern=None if animate else "dashed_medium",
        entryX=entryX,
        entryY=entryY,
        exitX=exitX,
        exitY=exitY,
        line_end_source="oval" if source.cctype == "Unit" else None,
        strokeColor=strokeColor,
    )
