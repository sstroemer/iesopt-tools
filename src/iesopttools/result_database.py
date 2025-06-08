import pandas as pd
import duckdb


class ResultDatabase:
    def __init__(self):
        pass

    def from_model(self, model, name: str | None = None, replace: bool = False):
        if name is None:
            name = model.internal.input.config["general"]["name"]
            name = f"{name['model']}_{name['scenario']}"
            name = "".join([c if c.isalnum() else "_" for c in name])

        self._parse_tags(model, name, replace)
        self._parse_carriers(model, name, replace)
        return name

    def _parse_tags(self, model, name: str, replace: bool):
        tags = []
        for (tag, components) in model.internal.model.tags.items():
            for component in components:
                tags.append((component, tag))

        tags = pd.DataFrame.from_records(tags, columns=["component", "tag"])
        tags = duckdb.from_df(tags)
        tags.create_view(name + "_tags", replace=replace)
    
    def _parse_carriers(self, model, name: str, replace: bool):
        carriers = []
        for component in model.get_components():
            cname = component.name
            ctype = iesopt.julia.typeof(component)
            ctype = str(ctype).split(".")[-1]

            if ctype == "Connection":
                carrier = component.carrier.name
                carriers.append((cname, "in", carrier))
                carriers.append((cname, "out", carrier))
            elif ctype == "Decision":
                pass
            elif ctype == "Node":
                carriers.append((cname, None, component.carrier.name))
            elif ctype == "Profile":
                if component.node_from is not None:
                    carriers.append((cname, "in", component.carrier.name))
                elif component.node_to is not None:
                    carriers.append((cname, "out", component.carrier.name))
            elif ctype == "Unit":
                for carrier in component.inputs.keys():
                    carriers.append((cname, "in", carrier.name))       
                for carrier in component.outputs.keys():
                    carriers.append((cname, "out", carrier.name))
            elif ctype == "Virtual":
                pass

        carriers = pd.DataFrame.from_records(carriers, columns=["component", "direction", "carrier"])
        carriers = duckdb.from_df(carriers)
        carriers.create_view(name + "_carriers", replace=replace)


rdb = ResultDatabase()

rdb.from_model(model, replace=True)

duckdb.view("my_model_my_scenario_tags")
duckdb.view("my_model_my_scenario_carriers)
