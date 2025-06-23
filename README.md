# IESopt Tools

Work in progress.

## Example usage

### Web UI

```python
model = iesopt.run(iesopt.make_example(iesopt.examples()[8], dst_dir="opt"))
rdb = RDB()

# First, start the UI, which will automatically open in your browser:
rdb.ui()

# Next, add the general entry that you can now inspect in the UI:
entry = rdb.add_entry(model)

# Then, try "materializing" the result of a select operation, which can then be used in the UI:
entry.select(components=["node1", "node2"], mode="shadowprice").to_table("prices")
```

### Converting to diagram/sketch

```python
import iesopt
from iesopttools import RDB
from iesopttools.diagrams import drawio


cfg = iesopt.make_example(iesopt.examples()[5], dst_dir="opt")
model = iesopt.run(cfg)

rdb = RDB()
entry = rdb.add_entry(model)

drawio.write_entry(entry, filename="opt/out/sketch.drawio")
```

### Plotting

```python
import iesopt
from iesopttools import RDB, Figure, Trace


model = iesopt.run(iesopt.make_example(iesopt.examples()[7], dst_dir="opt"))

rdb = RDB()
entry = rdb.add_entry(model)

fig = Figure(style="foo")
fig.add(Trace("line+markers+hv", entry.select(component="h2_north", mode="shadowprice"), name="North", sign=-1.0))
fig.add(Trace("bar", entry.select(component="h2_south", mode="shadowprice"), name="South", sign=-1.0))
fig.show()
```

#### Slicing figures on render

```python
fig.show(xslice=(0, 168))
```

#### Aggrgated values

```python
for asset in assets:
    fig.add(Trace("bar", entry.select(asset), aggregate_into=52))
```

#### Skipping traces

Empty traces are those that are `0` for all time steps, which can be skipped in the figure:

```python
fig = Figure(..., skip_empty=True)
```

### Querying injection/node

```python
assets = entry.query("carrier", "carrier = 'heat' AND direction = 'out' AND node IN ('grid_heat', 'grid_sim')")
```

Then also directly adding these elements as traces to a figure:

```python
for asset in assets:
    fig.add(Trace("bar", entry.select(asset)))
```
