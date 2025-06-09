# IESopt Tools

Work in progress.

## Example usage

### Plotting

```python
import iesopt
from iesopttools import RDB, Figure, Trace


model = iesopt.run(iesopt.make_example(iesopt.examples()[7], dst_dir="opt"))

rdb = RDB()
entry = rdb.add_entry(model)

fig = Figure(x=entry.snapshots, style="foo")
fig.add(Trace("line+markers+hv", entry.select(component="h2_north", mode="shadowprice"), name="North", sign=-1.0))
fig.add(Trace("bar", entry.select(component="h2_south", mode="shadowprice"), name="South", sign=-1.0))
fig.show()
```
