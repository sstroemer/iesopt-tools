import iesopt
import iesopttools as tools
import plotly.graph_objects as go


ex = iesopt.examples()
model = iesopt.run(iesopt.make_example(ex[7], dst_dir="opt"))

rdb = tools.RDB(replace_entries=True)
rdb.add_entry(model, "foo")

x = model.internal.model.snapshots
x = [x[i+1].name for i in range(len(x))]

rel = rdb["foo"].select(components=["h2_south", "h2_north"], mode="shadowprice")
trace_info = rel.explore(["component", "fieldtype", "field"]).df()

traces = []
for trace in trace_info.itertuples():
    trace_data = rel.select(component=trace.component, fieldtype=trace.fieldtype, field=trace.field).df()
    trace_data.set_index("snapshot", inplace=True)
    trace_data = (-1) * trace_data.loc[x, "value"].values  # TODO: allow configuring the sign of the shadow price

    traces.append(go.Bar(x=x, y=trace_data, name=trace.component))

fig = go.Figure(data=traces)
fig.show()
