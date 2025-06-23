import drawpyo
from ..layout.basic import Graph


class Sheet:
    def __init__(self, file, name="sheet_1"):
        self.page = drawpyo.Page(file, name=name)
        self.objects = dict()
        self.graph = Graph()

    def add_object(self, core_component):
        if core_component.obj.value in self.objects:
            raise ValueError(f"Object with name {core_component.obj.value} already exists in this sheet.")
        self.objects[core_component.obj.value] = core_component
        self.page.add_object(core_component.obj)
