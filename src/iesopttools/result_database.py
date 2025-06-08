import pandas as pd
import duckdb
import iesopt


class RDB:
    def __init__(self, replace_entries: bool = False):
        self.entries = dict()
        self.replace_entries = replace_entries

    def add_entry(self, model, name: str | None = None):
        if name is None:
            name = model.internal.input.config["general"]["name"]
            name = f"{name['model']}_{name['scenario']}"
            name = "".join([c if c.isalnum() else "_" for c in name])

        if (name in self.entries) and (not self.replace_entries):
            raise ValueError(f"RDBEntry '{name}' already exists. Use `replace_entries=True` to allow overwriting existing entries.")
        
        self.entries[name] = RDBEntry(model, name, replace=self.replace_entries)


class RDBEntryRelation:
    def __init__(self, parent, relation):
        self._parent = parent
        self._relation = relation
    
    def __repr__(self):
        return self._relation.__repr__()
    
    def __str__(self):
        return self._relation.__str__()

    def df(self):
        """Fetch data as `pandas.DataFrame`."""
        return self._relation.to_df()

    def duckdb(self):
        """Return the underlying `duckdb.DuckDBPyRelation`."""
        return self._relation
    
    def select(self, *args, limit: int | None = None, offset: int = 0, debug = False, **kwargs):
        rel = self._relation

        # Setup list of filters to apply.
        filters: list[str] = kwargs.pop("filters", [])

        # Check limit and offset.
        if (offset != 0) and (limit is None):
            raise ValueError("You cannot specify an offset without a limit; please specify both or neither.")
        
        # Create filters based on mode selection.
        mode = kwargs.pop("mode", "primal")
        if mode == "primal":
            filters.append("mode = 'primal'")
        elif mode == "dual":
            filters.append("mode = 'dual'")
        elif mode == "both":
            pass
        elif mode == "shadowprice":
            # This filters for dual results only, then for either "var.value" (Decision) or "con.nodalbalance" (Node).
            filters.append("mode = 'dual'")
            filters.append("(fieldtype = 'var' AND field = 'value') OR (fieldtype = 'con' AND field = 'nodalbalance')")
        else:
            raise ValueError(f"Invalid mode: {mode}; must be one of [primal, dual, both, shadowprice].")

        # Add positional arguments as QoL way to include filters.
        if len(args) > 0:
            for arg in args:
                if isinstance(arg, str):
                    filters.append(arg)
                else:
                    raise ValueError(f"Invalid argument type: {type(arg)}; positional arguments only allow for strings that describe a DuckDB relational filter expression.")
        
        # Create filters based on singular or plural selectors on the columns.
        selectors = ["snapshot", "component", "fieldtype", "field"]
        for selector in selectors:
            sel_sg = selector
            sel_pl = selector + "s"

            if (sel_sg in kwargs) and (sel_pl in kwargs):
                raise ValueError(f"You cannot specify both '{sel_sg}' and '{sel_pl}' at the same time.")
            
            if sel_sg in kwargs:
                val = kwargs.pop(sel_sg)
                filters.append(f"{sel_sg} = '{val}'")
            
            if sel_pl in kwargs:
                vals = kwargs.pop(sel_pl)
                if not isinstance(vals, (list, tuple)):
                    raise ValueError(f"The '{sel_pl}' argument must be a list or tuple of strings.")
                vals = "', '".join(vals)
                filters.append(f"{sel_sg} IN ('{vals}')")
        
        # Check for unexpected keyword arguments.
        if len(kwargs) != 0:
            raise ValueError(f"Unexpected keyword arguments: {', '.join(kwargs.keys())}")
        
        if debug:
            cnt = 0
            print(f"STEP {cnt:03d}: starting to apply filters on initial data:")
            print(rel)
            cnt += 1

        # Apply filters to the relation.
        for filter in filters:
            if debug:
                print(f"STEP {cnt:03d}: trying to apply filter \"{filter}\"")
            try:
                rel = rel.filter(filter)
                
                if debug:
                    print(f"STEP {cnt:03d}: filter applied successfully, current selection:")
                    print(rel)
                    cnt += 1
            except Exception as e:
                msg = str(e)
                msg = msg.replace("\n", "\n               ")

                raise Exception(
                    f"An error occurred while trying to apply a filter in `select()`.\n"
                    f"--------------------------------------------------------------------------\n\n"
                    f"    Filter:    {filter}\n"
                    f"    Type:      {type(e)}\n"
                    f"    Message:   {msg}\n\n"
                    f"--------------------------------------------------------------------------"
                ) from None
        
        # Check for columns to keep:
        # Project the relation to the relevant columns -- only keep mode when it can contain both primal and dual results.
        columns = ["snapshot", "component", "fieldtype", "field", "value"] + (["mode"] if mode == "both" else [])
        columns = [c for c in columns if c in rel.columns]
        columns = ", ".join(columns)
        
        # Project onto the selected columns.
        rel = rel.project(columns)
        
        # Apply limit and offset if requested.
        # NOTE: This requires sorting, otherwise the results might not be deterministic, which takes time.
        if limit is not None:
            # Apply limit and offset.
            rel = rel.order(columns).limit(limit, offset)
        
        return RDBEntryRelation(self._parent, rel)

    def explore(self, *args, order: bool = True, **kwargs):
        rel = self._relation

        # Collect all projections to apply.
        projections = []

        if len(args) == 0:
            # Check for kwarg based projections (e.g., `snaphots=True`).
            for key in ["snapshot", "component", "fieldtype", "field", "value", "mode"]:
                has_sg = key in kwargs
                has_pl = (key + "s") in kwargs
                
                if has_sg and has_pl:
                    raise ValueError(f"You cannot specify both '{key}' and '{key}s' at the same time.")
                
                if has_sg or has_pl:
                    projections.append(kwargs.pop(key if has_sg else key + "s"))
        elif len(args) == 1:
            # Check for positional arguments, e.g., `explore("fields")` or `explore(["fields", "modes"])`.
            arg = args[0]

            if len(kwargs) > 0:
                raise ValueError(
                    "You cannot specify both positional arguments and keyword arguments at the same time. "
                    "This most likely means that you tried using something like `explore(\"fields\", modes=True)`. "
                    "Instead, either use `explore(\"fields\")` to only explore the 'fields' column, use "
                    "`explore(modes=True)` to only explore the 'modes' column, or use either "
                    "`explore(fields=True, modes=True)` or `explore([\"modes\", \"fields\"])` to explore distinct "
                    "combinations both columns at the same time."
                )

            if isinstance(arg, str):
                arg = arg[:-1] if arg.endswith("s") else arg
                if arg not in ["snapshot", "component", "fieldtype", "field", "mode"]:
                    raise ValueError(f"Invalid argument: {arg}; must be one of ['snapshots', 'components', 'fieldtypes', 'fields', 'modes'], or the singular form of one of those.")
                projections.append(arg)
            elif isinstance(arg, list):
                arg = [a[:-1] if a.endswith("s") else a for a in arg]
                for a in arg:
                    if a not in ["snapshot", "component", "fieldtype", "field", "mode"]:
                        raise ValueError(f"Cannot explore unknown dimension/column '{a}'; valid ones are ['snapshots', 'components', 'fieldtypes', 'fields', 'modes'], or their singular forms.")
                projections.extend(arg)
            else:
                raise ValueError(f"Invalid argument type: {type(arg)}; positional arguments only allow for a single string that describes a column name to explore.")
        else:
            raise ValueError("The `explore()` function excepts either positional arguments or keyword arguments, but not both at the same time.")
        
        if "value" in projections:
            raise ValueError("You cannot explore the 'value' column; it is not a dimension but a result value.")
        
        if len(kwargs) > 0:
            raise ValueError(f"Unexpected keyword arguments: {', '.join(kwargs.keys())}")
        
        # Enforce a certain order of projections.
        projections = [p for p in ["snapshot", "component", "fieldtype", "field", "mode"] if p in projections]
        projections = ", ".join(projections)

        # Project and select distinct values.
        rel = rel.project(projections)
        rel = rel.distinct()

        # If requested, order the relation subsequently by each columns (ascending).
        if order:
            rel = rel.order(projections)

        return RDBEntryRelation(self._parent, rel)

class RDBEntry:
    def __init__(self, model, name: str | None = None, replace: bool = False):     
        self.name = name
        self.model = model
        
        self.tags = self._parse_tags(replace)
        self.carriers = self._parse_carriers(replace)
        self.rel = RDBEntryRelation(self, duckdb.from_df(model.results.to_pandas()))

    def select(self, *args, limit: int | None = None, offset: int = 0, debug = False, **kwargs):
        return self.rel.select(*args, limit=limit, offset=offset, debug=debug, **kwargs)

    def explore(self, *args, order: bool = True, **kwargs):
        return self.rel.explore(*args, order=order, **kwargs)

    def _parse_tags(self, replace: bool):
        tags = []
        for (tag, components) in self.model.internal.model.tags.items():
            for component in components:
                tags.append((component, tag))

        tags = pd.DataFrame.from_records(tags, columns=["component", "tag"])
        tags = duckdb.from_df(tags)
        return tags.create_view(self.name + "_tags", replace=replace)
    
    def _parse_carriers(self, replace: bool):
        carriers = []
        for component in self.model.get_components():
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
        return carriers.create_view(self.name + "_carriers", replace=replace)


rdb = RDB(replace_entries=True)

rdb.add_entry(model)

# duckdb.view("my_model_my_scenario_tags")
# duckdb.view("my_model_my_scenario_carriers")

# rdb.entries["my_model_my_scenario"].rel["tags"]

# rdb.entries["my_model_my_scenario"].explore("components")


rdb.entries["my_model_my_scenario"].explore("field")
