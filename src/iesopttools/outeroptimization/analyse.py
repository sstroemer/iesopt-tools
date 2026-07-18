import json
import itertools
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.interpolate import griddata


def heatmap(folder_path):
    folder = Path(folder_path)
    (folder / "plots").mkdir(parents=True, exist_ok=True)

    with open(folder / "intermediate.json") as f:
        intermediate = json.load(f)

    with open(folder / "result.json") as f:
        result = json.load(f)

    best_params = result["parameters"]

    # Extract parameters and objectives from each entry
    entries = []
    for entry in intermediate:
        entries.append({
            "parameters": entry["parameters"],
            "objectives": entry["objectives"],
        })

    # Get all parameter keys
    param_keys = list(entries[0]["parameters"].keys())
    objective_keys = ["total", "outer", "iesopt"]

    # For each combination of two parameters, create a figure
    for p1, p2 in itertools.combinations(param_keys, 2):
        # Get unique sorted values for each parameter
        vals_p1 = sorted(set(e["parameters"][p1] for e in entries))
        vals_p2 = sorted(set(e["parameters"][p2] for e in entries))

        # Build grids for each objective
        grids = {obj: np.full((len(vals_p2), len(vals_p1)), np.nan) for obj in objective_keys}

        idx_p1 = {v: i for i, v in enumerate(vals_p1)}
        idx_p2 = {v: i for i, v in enumerate(vals_p2)}

        for entry in entries:
            i = idx_p1[entry["parameters"][p1]]
            j = idx_p2[entry["parameters"][p2]]
            for obj in objective_keys:
                grids[obj][j, i] = entry["objectives"][obj]

        # Best solution marker position
        best_x = idx_p1.get(best_params.get(p1))
        best_y = idx_p2.get(best_params.get(p2))

        fig, axes = plt.subplots(1, 3, figsize=(15, 5))

        # Collect all points for interpolation using actual parameter values
        points = np.array([[e["parameters"][p1], e["parameters"][p2]] for e in entries])
        grid_x, grid_y = np.meshgrid(
            np.linspace(vals_p1[0], vals_p1[-1], min(200, len(vals_p1) * 10)),
            np.linspace(vals_p2[0], vals_p2[-1], min(200, len(vals_p2) * 10)),
        )

        # Best solution marker position using actual values
        best_x = best_params.get(p1)
        best_y = best_params.get(p2)

        for ax, obj in zip(axes, objective_keys):
            values = np.array([e["objectives"][obj] for e in entries])
            grid_z = griddata(points, values, (grid_x, grid_y), method="linear")

            im = ax.contourf(grid_x, grid_y, grid_z, levels=20, cmap="viridis")
            ax.set_title(f"objective: {obj}")
            ax.set_xlabel(p1)
            ax.set_ylabel(p2)
            plt.colorbar(im, ax=ax)

            if best_x is not None and best_y is not None:
                ax.plot(best_x, best_y, "kx", markersize=12, markeredgewidth=2.5)

        plt.tight_layout()
        plt.savefig(folder / "plots" / f"heatmap_{p1}_{p2}.png", dpi=300)

heatmap("iesopt-tools-outer-optimization/20260718T151747")
plt.show()
