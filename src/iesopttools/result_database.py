import pandas as pd
import duckdb
import iesopt


class RDB:
    """
    A class to manage a result database (RDB) of entries, each representing a model result.
    """
    def __init__(self, replace_entries: bool = False):
        self._entries = dict()
        self._replace_entries = replace_entries

    def __getitem__(self, name: str) -> "RDBEntry":
        """Get an entry by name."""
        if name not in self._entries:
            raise KeyError(f"RDBEntry '{name}' does not exist.")
        return self._entries[name]
    
    @property
    def entries(self) -> list[str]:
        """Get a list of all entry names."""
        return sorted(self._entries.keys())

    def add_entry(self, model, name: str | None = None) -> "RDBEntry":
        """
        Add a new entry to the RDB based on the provided (solved) model.
        If `name` is not provided, it will be derived from the model's internal configuration.
        """
        if name is None:
            name = model.internal.input.config["general"]["name"]
            name = f"{name['model']}_{name['scenario']}"
            name = "".join([c if c.isalnum() else "_" for c in name])

        if (name in self._entries) and (not self._replace_entries):
            raise ValueError(f"RDBEntry '{name}' already exists. Use `replace_entries=True` to allow overwriting existing entries.")
        
        entry = RDBEntry(model, name, replace=self._replace_entries)
        self._entries[name] = entry
        return entry


class RDBEntryRelation:
    """
    A class to represent a relation in the result database (RDB).
    It provides methods to select, explore, and evaluate data from the relation.
    """
    def __init__(self, parent, relation):
        self._parent = parent
        self._relation = relation
    
    def __repr__(self):
        return self._relation.__repr__()
    
    def __str__(self):
        return self._relation.__str__()

    def df(self) -> pd.DataFrame:
        """Fetch data as `pandas.DataFrame`."""
        return self._relation.to_df()

    def duckdb(self) -> duckdb.DuckDBPyRelation:
        """Return the underlying `duckdb.DuckDBPyRelation`."""
        return self._relation
    
    def select(self, *args, limit: int | None = None, offset: int = 0, debug = False, **kwargs) -> "RDBEntryRelation":
        """
        Select data from the relation based on the provided arguments. This method allows for filtering by `snapshot`,
        `component`, `fieldtype`, `field`, or `mode`. The `mode` can be one of `primal`, `dual`, `both`, or
        `shadowprice`. The `shadowprice` mode filters for dual results only, then for either `var.value` (Decision) or
        `con.nodalbalance` (Node).
        """
        rel = self._relation

        # Setup list of filters to apply.
        filters: list[str] = kwargs.pop("filters", [])

        # Check limit and offset.
        if (offset != 0) and (limit is None):
            raise ValueError("You cannot specify an offset without a limit; please specify both or neither.")
        
        # Create filters based on mode selection.
        mode = None
        if "mode" not in rel.columns:
            if "mode" in kwargs:
                raise ValueError("The 'mode' column is not present in the relation; you cannot filter by mode.")
        else:
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
                if not isinstance(val, str):
                    raise ValueError(f"The `{sel_sg}` argument must be a string; if you are trying to filter based on multiple options, pass `{sel_pl} = ...` instead.")
                filters.append(f"{sel_sg} = '{val}'")
            
            if sel_pl in kwargs:
                vals = kwargs.pop(sel_pl)
                if isinstance(vals, RDBEntryQuery):
                    vals = vals.fetch()
                if not isinstance(vals, (list, tuple)):
                    raise ValueError(f"The `{sel_pl}` argument must be a list or tuple of strings.")
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

    def explore(self, *args, order: bool = True, **kwargs) -> "RDBEntryRelation":
        """
        Explore the relation by projecting and selecting distinct values of the specified columns. This method allows
        for exploring the dimensions of the relation, such as `snapshot`, `component`, `fieldtype`, `field`, and `mode`.
        The `order` parameter determines whether the results should be ordered by the projected columns.
        The method can be called with positional arguments (e.g., `explore("fields")`) or keyword arguments (e.g.,
        `explore(fields=True)`).        
        """
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
        if len(projections) > 0:
            projections = [p for p in ["snapshot", "component", "fieldtype", "field", "mode"] if p in projections]
            projections = ", ".join(projections)
        else:
            # Project onto existing columns.
            projections = [c for c in ["snapshot", "component", "fieldtype", "field", "mode"] if c in rel.columns]
            projections = ", ".join(projections)

        # Project and select distinct values.
        if projections != "":
            rel = rel.project(projections)
        rel = rel.distinct()

        # If requested, order the relation subsequently by each columns (ascending).
        if order and (projections != ""):
            rel = rel.order(projections)

        return RDBEntryRelation(self._parent, rel)

    def evaluate(self, f: str | list[str], *, by: str | list[str] | None = None) -> "RDBEntryRelation":
        """
        Evaluate the relation by applying an aggregation function `f` to the `value` column.
        The function can be a single string (e.g., "sum", "mean", "quantile(0.9)") or a list of strings.
        The `by` parameter can be a single string or a list of strings to group the results by.
        If `by` is not provided, the aggregation will be applied to the entire relation.
        The result will be a new relation with the aggregated values.
        If `f` contains parentheses, it is assumed to be a function with arguments (e.g., "quantile(0.9)").
        If `f` is a list, each function will be applied separately, and the results will be combined.
        If `by` is a list, the results will be grouped by each of the specified columns.
        If `by` is a single string, the results will be grouped by that column.
        If `by` is not provided, the results will be aggregated without grouping.
        If `f` is a string without parentheses, it is assumed to be a simple aggregation function (e.g., "sum", "mean").
        If `f` is a list, each function will be applied separately, and the results will be combined.
        The result will be a new relation with the aggregated values.
        """
        f = f if isinstance(f, list) else [f]
        aggr = []
        group = []

        if by:
            aggr.extend(by if isinstance(by, list) else [by])
            group.extend(by if isinstance(by, list) else [by])
        
        for fi in f:
            if "(" in fi:
                if not fi.endswith(")"):
                    raise ValueError(f"Invalid aggregation function '{fi}'; when supplying arguments, make sure they are give like `quantile(0.9)`.")
                fi, fiargs = fi[:-1].split("(")
                aggr.append(f"{fi}(value, {fiargs}) AS '{fi}({fiargs})'")
            else:
                aggr.append(f"{fi}(value) AS {fi}")

        return RDBEntryRelation(self._parent, self._relation.aggregate(", ".join(aggr), ", ".join(group)))

class RDBEntryQuery:
    """
    A class to query components based on their `tag` or `carrier` information.
    It allows for filtering and union/intersection operations on the queried components.
    The relation can be used to fetch the components as a list or to perform further operations.
    The relation can be either `tag` or `carrier`, and the filter can be a string to filter the components.
    """
    def __init__(self, entry, relation: str, filter: str = "*"):
        self._entry = entry
        
        if relation.startswith("tag"):
            self._relation = entry.tags
        elif relation.startswith("carrier"):
            self._relation = entry.carriers
        elif relation == "__none__":
            return
        else:
            raise ValueError(f"Invalid relation: {relation}; must be on of [tag, carrier], or the plural form of one of those.")
        
        self._relation = self._relation.filter(filter).project("component").distinct()

    def __repr__(self):
        return self._relation.__repr__()
    
    def __str__(self):
        return self._relation.__str__()

    def fetch(self) -> list[str]:
        """Fetch data as `list`."""
        return [el[0] for el in self._relation.fetchall()]

    def duckdb(self) -> duckdb.DuckDBPyRelation:
        """Return the underlying `duckdb.DuckDBPyRelation`."""
        return self._relation
    
    def union(self, relation: str, filter: str = "*") -> "RDBEntryQuery":
        """
        Union this query with another query based on the specified relation and filter.
        The result will be a new query that combines the components from both queries.
        The union will be distinct, meaning that duplicate components will be removed.
        """
        other = RDBEntryQuery(self._entry, relation, filter)
        new = RDBEntryQuery(self._entry, "__none__")
        new._relation = self._relation.union(other.duckdb())
        new._relation = new._relation.distinct()  # `union` can create duplicates, so we remove them.
        return new

    def intersect(self, relation: str, filter: str = "*") -> "RDBEntryQuery":
        """
        Intersect this query with another query based on the specified relation and filter.
        The result will be a new query that contains only the components that are present in both queries.
        """
        other = RDBEntryQuery(self._entry, relation, filter)
        new = RDBEntryQuery(self._entry, "__none__")
        new._relation = self._relation.intersect(other.duckdb())
        return new

class RDBEntry:
    """
    A class representing a single entry in the result database (RDB).
    It contains the model results and provides methods to explore and query the data.
    """
    def __init__(self, model, name: str | None = None, replace: bool = False):     
        self.name = name

        self.tags = self._parse_tags(model, replace)
        self.carriers = self._parse_carriers(model, replace)
        
        # Manually keep snapshots (in correct order).
        self.snapshots = model.internal.model.snapshots
        self.snapshots = [self.snapshots[t].name for t in range(1, len(self.snapshots) + 1)]

        results = model.results.to_pandas()
        self.rel = RDBEntryRelation(self, duckdb.from_df(results))

    def explore(self, *args, order: bool = True, **kwargs) -> RDBEntryRelation:
        """
        Explore the entry by projecting and selecting distinct values of the specified columns.
        """
        if (len(args) == 1) and args[0].startswith("tag"):
            return self.tags.project("tag").distinct()

        if (len(args) == 1) and args[0].startswith("carrier"):
            return self.carriers.project("carrier").distinct()

        return self.rel.explore(*args, order=order, **kwargs)

    def query(self, relation: str, filter: str = "*") -> RDBEntryQuery:
        """
        Query components, either based on filtering `tag` or `carrier` information.
        """
        return RDBEntryQuery(self, relation, filter)

    def select(self, *args, limit: int | None = None, offset: int = 0, debug = False, **kwargs) -> RDBEntryRelation:
        """
        Select data from the entry based on the provided arguments.
        This method allows for filtering by `snapshot`, `component`, `fieldtype`, `field`, or `mode`.
        """
        return self.rel.select(*args, limit=limit, offset=offset, debug=debug, **kwargs)

    def _parse_tags(self, model, replace: bool) -> duckdb.DuckDBPyRelation:
        """
        Parse tags from the model and create a DuckDB view for them. Each tag is associated with its components.
        The view will be named `<entry_name>_tags`. If `replace` is True, it will replace any existing view with the
        same name.
        """
        tags = []
        for (tag, components) in model.internal.model.tags.items():
            for component in components:
                tags.append((component, tag))

        tags = pd.DataFrame.from_records(tags, columns=["component", "tag"])
        tags = duckdb.from_df(tags)
        return tags.create_view(self.name + "_tags", replace=replace)
    
    def _parse_carriers(self, model, replace: bool) -> duckdb.DuckDBPyRelation:
        """
        Parse carriers from the model and create a DuckDB view for them. Each carrier is associated with its components.
        The view will be named `<entry_name>_carriers`. If `replace` is True, it will replace any existing view with the
        same name.
        """
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
        return carriers.create_view(self.name + "_carriers", replace=replace)


# rdb = RDB(replace_entries=True)
# rdb.add_entry(model)
# rdb.entries
# entry = rdb["my_model_my_scenario"]

# entry.select(components=["chp.heat", "chp.power"], field="in_gas").evaluate("sum", by="component")

# entry.explore("components")
# entry.explore("tags")
# entry.explore("carriers")

# entry.select(component="create_gas")
# entry.select(component="create_gas").explore("field")  # most of these support either `field` or `fields` (so both singular and plural forms)
# entry.select(component="create_gas").explore(["fieldtype", "field"])

# entry.select(mode="shadowprice").explore("component")
# entry.select(mode="shadowprice").evaluate("mean", by="component")

# q0 = entry.query("tag", "tag = 'Profile'")
# q1 = q0.intersect("carrier", "direction = 'out' AND carrier = 'electricity'")
# q2 = q0.union("carrier", "direction = 'out' AND carrier = 'electricity'")

# either call `fetch()` to get a list of components, or use it directly in `select()`:
# entry.select(components=q1.fetch).evaluate("sum", by="component")

# entry.rel.duckdb().aggregate("quantile(value, 0.8) AS 'quantile(0.8)'")

# duckdb.view("my_model_my_scenario_tags")
# duckdb.view("my_model_my_scenario_carriers")


# entry.explore("components")
# entry.explore("fields")
# entry.select(component="create_gas")
# entry.select(component="create_gas").explore("fields")
# entry.select(components=["plant_gas", "plant_solar"], field="in_gas")
# entry.select(components=["chp.heat", "chp.power"], field="in_gas")


# # Annual sum of energy consumed by component.
# entry.select(components=["chp.heat", "chp.power"], field="in_gas").evaluate("sum", by="component")

# # Per-snapshot sum of energy (of all components).
# entry.select(field="in_gas").evaluate("sum", by="snapshot")

# # Per-snapshot and per-component energy mix.
# entry.select(field="in_gas").evaluate("sum", by=["snapshot", "component"])

# # Total (annual) energy in/out mix of all components. (TODO: this "misses" profiles for example)
# entry.select("field SIMILAR TO '(in|out)_.*'").evaluate("sum", by="field")

# # Total, per-snapshot, energy in/out mix of all components.
# entry.select("field SIMILAR TO '(in|out)_.*'").evaluate("sum", by="snapshot, field")

# min, max, sum, mean, median, quantile, stddev, variance, mode, histogram

# entry.select(field="in_gas").evaluate("quantile_cont(0.5)")
# entry.select(field="in_gas").evaluate("histogram", by="component") # .duckdb().fetchone()
