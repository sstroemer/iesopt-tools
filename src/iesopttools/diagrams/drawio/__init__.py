import drawpyo
from pathlib import Path

from .sheet import Sheet
from .components import *
from .util import connect


def write_entry(entry, filename: str):
    diagram = drawpyo.File()
    sheet = Sheet(diagram)

    for row in entry.explore("components"):
        tags = entry.query("tags", f"component = '{row.component}'").to_df()["tag"].tolist()
        if len(tags) > 1:
            print(
                f"Multiple tags (currently not supported) found for component {row.component}: {tags}; "
                f"ignoring all except the first one."
            )
            tags = [t for t in tags if t in ["Profile", "Node", "Unit", "Connection", "Decision"]]
        tag = tags[0]

        carriers = entry.query("carriers", f"component = '{row.component}'").to_df()

        if tag == "Profile":
            assert len(carriers) == 1
            Profile(carriers["carrier"].iloc[0], name=row.component).add_to(sheet)
        elif tag == "Node":
            assert len(carriers) == 1
            # TODO: Simplify that check
            has_state = bool(len(entry.select(component=row.component, field="state").to_duckdb()))
            Node(carriers["carrier"].iloc[0], name=row.component, has_state=has_state).add_to(sheet)
        elif tag == "Unit":
            Unit(name=row.component).add_to(sheet)
        elif tag == "Connection":
            continue
        else:
            # TODO: Handle virtuals here.
            continue

    edges = entry.query("carriers", "direction IS NOT NULL")
    connections = entry.query("tags", "tag = 'Connection'").to_df()["component"].tolist()

    for row in edges:
        if row.component in connections:
            if row.direction == "out":
                continue

            df = edges.to_duckdb().filter(f"component = '{row.component}'").to_df()
            df.set_index("direction", inplace=True)
            Connection(
                source=sheet.objects[df.loc["in", "node"]],
                target=sheet.objects[df.loc["out", "node"]],
                name=row.component,
            ).add_to(sheet)
        elif row.direction == "out":
            connect(sheet.objects[row.component], sheet.objects[row.node])
            sheet.graph.add_edge(row.component, row.node)  # TODO: the connect above could be automated
        elif row.direction == "in":
            connect(sheet.objects[row.node], sheet.objects[row.component])
            sheet.graph.add_edge(row.node, row.component)  # TODO: the connect above could be automated

    layout = sheet.graph.layout()
    for k, v in layout.items():
        sheet.objects[k].obj.position = v

    filename = Path(filename)
    filename.parent.mkdir(parents=True, exist_ok=True)
    diagram.write(file_path=filename.parent, file_name=filename.name)
