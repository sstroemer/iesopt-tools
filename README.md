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
