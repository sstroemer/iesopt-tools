import drawpyo

from .sheet import Sheet
from .util import COLORS


class CoreComponent:
    def __init__(self):
        pass

    @property
    def cctype(self):
        return self.__class__.__name__

    def add_to(self, sheet):
        if not isinstance(sheet, Sheet):
            raise TypeError("sheet must be an instance of Sheet")
        sheet.add_object(self)
        self.obj.page = sheet.page

        if self.cctype in ["Node", "Profile", "Unit"]:
            sheet.graph.add_vertex(self.obj.value, self.cctype)
        elif self.cctype == "Connection":
            sheet.graph.add_edge(self.source, self.target)

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
        self.source = source.obj.value
        self.target = target.obj.value
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
        self.obj._add_and_set_style_attrib("strokeWidth", 3.0)  # TODO: "1.5" does not work
