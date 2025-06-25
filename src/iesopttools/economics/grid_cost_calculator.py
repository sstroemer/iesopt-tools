import pandas as pd
from datetime import datetime
_special_assets = ['pumped_hydro', 'p2g']
_special_use_cases = ['aFRR', 'mFRR']


def calculate_grid_costs(dso: str, grid_level: int, *, grid_feedin_rated_power_leq5MW: bool, year: int | None = None):

    year = year if year else datetime.now().year

    grid_charge_data = pd.read_csv(f'grid_charge_data/{dso}_{year}.csv', delimiter=';')

    grid_column = f"NE{grid_level}"

    output_rows = []

    for _, row in grid_charge_data.iterrows():
        component = row['component']
        component_type = row['type']
        direction = row['direction']
        unit = row['unit']
        value = row.get(grid_column, 0)

        row_out = {
            "component": component,
            "type": component_type,
            "consumption": 0,
            "injection": 0,
            "unit": unit
        }

        if direction == "consumption":
            row_out["consumption"] = value
        elif direction == "feedin":
            row_out["injection"] = value if grid_feedin_rated_power_leq5MW else 0
        elif direction == "both":
            row_out["consumption"] = value
            row_out["injection"] = value if grid_feedin_rated_power_leq5MW else 0

        output_rows.append(row_out)

    grid_costs_df = pd.DataFrame(output_rows)
    grid_costs_df_grouped = grid_costs_df.groupby(["component", "type", "unit"], as_index=False).sum(numeric_only=True)

    return grid_costs_df_grouped[["component", "type", "consumption", "injection", "unit"]]


if __name__ == '__main__':
    output = calculate_grid_costs(dso="Wiener_Netze", grid_level=5, grid_feedin_rated_power_leq5MW=True, year=2025)
    print(output.to_csv(index=False))
