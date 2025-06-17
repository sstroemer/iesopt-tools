import drawpyo
import iesopt
from iesopttools import RDB
ex = iesopt.examples()
cfg = iesopt.make_example(ex[16], dst_dir="opt")
model = iesopt.run(cfg)
rdb = RDB()
entry = rdb.add_entry(model, "foo")


COLORS = {
    "electricity": "#4c00ff",
    "heat": "#7a1800",
}

class Sheet:
    def __init__(self, file, name="sheet_1"):
        self.page = drawpyo.Page(file, name=name)
        self.objects = dict()
    
    def add_object(self, core_component):
        if core_component.obj.value in self.objects:
            raise ValueError(f"Object with name {core_component.obj.value} already exists in this sheet.")
        self.objects[core_component.obj.value] = core_component
        self.page.add_object(core_component.obj)

class CoreComponent:
    def __init__(self):
        pass

    def add_to(self, sheet):
        if not isinstance(sheet, Sheet):
            raise TypeError("sheet must be an instance of Sheet")
        sheet.add_object(self)
        self.obj.page = sheet.page
        return self

class Profile(CoreComponent):
    def __init__(self, carrier, name):
        super().__init__()
        self.obj = drawpyo.diagram.Object(name)
        self.obj.apply_style_string("rhombus;whiteSpace=wrap;html=1;")
        self.obj.width = 80
        self.obj.height = 80
        self.obj.strokeColor = COLORS.get(carrier, "#ff00ff")

class Node(CoreComponent):
    def __init__(self, carrier, name, *, has_state=False):
        super().__init__()
        self.obj = drawpyo.diagram.Object(name)
        self.obj.apply_style_string("rounded=1;whiteSpace=wrap;html=1;arcSize=50;")
        self.obj.width = 80
        self.obj.height = 40
        self.obj.strokeColor = COLORS.get(carrier, "#ff00ff")
        if has_state:
            self.obj.fillColor = COLORS.get(carrier, "#ff00ff")
            self.obj._add_and_set_style_attrib("fillOpacity", 15)

class Unit(CoreComponent):
    def __init__(self, name):
        super().__init__()
        self.obj = drawpyo.diagram.Object(name)
        self.obj.apply_style_string("shape=hexagon;perimeter=hexagonPerimeter2;whiteSpace=wrap;html=1;fixedSize=1;")
        self.obj.width = 120
        self.obj.height = 60
        self.obj.strokeColor = "#505050"

class Connection(CoreComponent):
    def __init__(self, source, target, name):
        super().__init__()
        self.obj = drawpyo.diagram.Edge(
            source=source.obj,
            target=target.obj,
            label_offset=0,
            label=name,
            jetty_size=40,
            rounded=True,
            jumpStyle="gap",
            strokeColor=source.obj.strokeColor,
        )
        self.obj._add_and_set_style_attrib("strokeWidth", 1.5)  # TODO: does not work

def connect(source, target, animate=False):
    entryX = exitX = entryY = exitY = None
    strokeColor = None

    if isinstance(source, Unit):
        exitX = 1.0
        exitY = 0.5
        strokeColor = target.obj.strokeColor
    elif isinstance(target, Unit):
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
        line_end_source="oval" if isinstance(source, Unit) else None,
        strokeColor=strokeColor,
    )

diagram = drawpyo.File()
sheet = Sheet(diagram)

for row in entry.explore("components"):
    tags = entry.query("tags", f"component = '{row.component}'").to_df()["tag"].tolist()
    if len(tags) > 1:
        print(f"Multiple tags (currently not supported) found for component {row.component}: {tags}; ignoring all except the first one.")
    tag = tags[0]

    carriers = entry.query("carriers", f"component = '{row.component}'").to_df()

    if tag == "Profile":
        assert len(carriers) == 1
        Profile(carriers["carrier"].iloc[0], name=row.component).add_to(sheet)
    elif tag == "Node":
        assert len(carriers) == 1
        Node(carriers["carrier"].iloc[0], name=row.component).add_to(sheet)   # TODO: has_state
    elif tag == "Unit":
        Unit(name=row.component).add_to(sheet)
    elif tag == "Connection":
        print(f"Skipping {row.component} as it is a Connection.")
        continue
    else:
        print(f"Unknown tag {tag} for component {row.component}; skipping.")
        continue


# profiles = [Profile("electricity", name=f"profile_{i}", page=page) for i in range(1, 5)]
# nodes = [Node("electricity", name=f"node_1", page=page), Node("heat", name=f"node_2", page=page, has_state=True), Node("electricity", name=f"node_3", page=page)]

# unit = Unit(name="unit_1", page=page)

# connect(profiles[0], nodes[0])
# connect(nodes[0], profiles[1])

# Connection(nodes[0], nodes[2], name="connection_1")

# connect(nodes[1], unit)
# connect(unit, nodes[0])

# object = drawpyo.diagram.Object(page=page)

# object = drawpyo.diagram.object_from_library(
#     library="general",
#     obj_name="process",
#     page=page,
#     )

# object.width = 220
# object.height = 80
# object.aspect = 'fixed'
# object.position = (200, 200)

# obj = drawpyo.diagram.Object(page=page)
# obj.apply_style_string("rhombus;whiteSpace=wrap;html=1;")
# obj.width = obj.height = 40

# parent = drawpyo.diagram.Object(page=page)
# parent.apply_style_string("document;whiteSpace=wrap;html=1;boundedLbl=1;")
# parent.autosize_to_children = True
# parent.add_object(obj)

# obj2 = drawpyo.diagram.Object(page=page)
# obj2.apply_style_string("rhombus;whiteSpace=wrap;html=1;")
# obj2.width = obj2.height = 40

# link = drawpyo.diagram.Edge(
#     page=page,
#     source=obj,
#     target=obj2,
#     label_offset=0,
#     label="test",
#     jetty_size=40,
#     rounded=True,
#     jumpStyle="gap",
#     # flowAnimation=True,
#     pattern="dashed_medium",
#     line_end_target="classicThin",
#     # exitX, exitY for Units, etc.
# )

# from Units
# line_end_source="oval"

diagram.write(file_path = "./", file_name = "test.drawio")
