import pandas as pd
import duckdb


class ResultDatabase:
    def __init__(self):
        pass

    def from_model(self, model, name: str | None = None):
        if name is None:
            name = model.internal.input.config["general"]["name"]
            name = f"{name['model']}_{name['scenario']}"
            name = "".join([c if c.isalnum() else "_" for c in name])

        self._parse_tags(model, name)
        return name

    def _parse_tags(self, model, name: str):
        tags = []
        for (tag, components) in model.internal.model.tags.items():
            for component in components:
                tags.append((component, tag))

        tags = pd.DataFrame.from_records(tags, columns=["component", "tag"])
        tags = duckdb.from_df(tags)
        tags.create_view(name + "_tags", replace=True)


rdb = ResultDatabase()

rdb.from_model(model)

duckdb.view("my_model_my_scenario_tags")

