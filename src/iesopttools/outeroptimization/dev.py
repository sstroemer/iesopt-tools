import iesopt
import numpy as np
import scipy.optimize as sciopt
from collections.abc import Callable
from pathlib import Path
from datetime import datetime
import json

# >>> model.results.overview(".*", temporal=False)
# build_pipeline  var  value          primal      0.750000
# build_gas       var  value          primal      0.607143
# build_storage   var  value          primal      0.450714
# dtype: float64
# obj = 2015.6428571 (invest: 1098.642858; operational: 917.000000)


def run_iesopt(parameters: dict) -> iesopt.Model:
    return iesopt.run("opt/config.iesopt.yaml", parameters={"cost_gas": 100.0} | parameters)


class OuterOptimization:
    def __init__(
        self,
        f_iesopt: Callable[[dict], iesopt.Model],
        *,
        method: str = "Nelder-Mead",
        path: str | Path = "iesopt-tools-outer-optimization",
    ):
        self._f_iesopt = f_iesopt
        self._method = method
        self._decisions = dict()
        self._results = []
        self._iteration = 0
        self._path = Path(path) / datetime.now().strftime("%Y%m%dT%H%M%S")

    def add_decision(
        self,
        name: str,
        *,
        cost: float | Callable[[float], float],
        lb: float | None = 0.0,
        ub: float | None = None,
        initial_guess: float | None = None,
    ) -> None:
        if name in self._decisions:
            raise ValueError(f"Decision variable '{name}' already exists.")
        if ((lb is None) or (ub is None)) and (initial_guess is None):
            raise ValueError(
                f"Decision variable '{name}' must have either both bounds or you need to provide a valid initial guess."
            )
        if ((lb is not None) and (ub is not None)) and (initial_guess is not None):
            if not (lb <= initial_guess <= ub):
                raise ValueError(
                    f"Initial guess for decision variable '{name}' must be within the bounds [{lb}, {ub}]."
                )
        self._decisions[name] = dict(cost=cost, lb=lb, ub=ub, initial_guess=initial_guess)

    def minimize(self):
        self._path.mkdir(parents=True, exist_ok=False)
        self._tojson("config", {"method": self._method, "decisions": self._decisions})

        if self._method == "Nelder-Mead":
            res = self._minimize_nelder_mead()
            self._tojson("result", {
                "message": res.message,
                "success": res.success,
                "objective_value": res.fun,
                "parameters": {name: value for name, value in zip(self._decisions.keys(), res.x)},
                "n_iter": res.nit,
                "n_eval": res.nfev,
                "simplex": res.final_simplex[0].tolist(),
            })
        else:
            raise ValueError(f"Unsupported optimization method: {self._method}")
        
       

        return res

    def _minimize_nelder_mead(self):
        x0 = [d["initial_guess"] for d in self._decisions.values()]
        bounds = tuple((d["lb"], d["ub"]) for d in self._decisions.values())
        res = sciopt.minimize(
            self._objective_function, x0, method="Nelder-Mead", tol=1e-4, callback=self._callback, bounds=bounds
        )
        return res

    def _objective_function(self, x):
        parameters = {name: value for name, value in zip(self._decisions.keys(), x)}

        obj_iesopt = np.inf
        obj_outer = sum(
            (d["cost"](parameters[name]) if callable(d["cost"]) else d["cost"] * parameters[name])
            for name, d in self._decisions.items()
        )

        try:
            model = self._f_iesopt(parameters)
            if model.status in [iesopt.ModelStatus.OPTIMAL, iesopt.ModelStatus.OPTIMAL_LOCALLY]:
                obj_iesopt = model.objective_value
        except Exception:
            pass

        obj_total = obj_iesopt + obj_outer
        self._results.append({
            "iteration": self._iteration,
            "parameters": parameters,
            "objectives": {"total": obj_total, "outer": obj_outer, "iesopt": obj_iesopt},
        })
        self._tojson("intermediate", self._results)

        return obj_total

    def _callback(self, intermediate_result: sciopt.OptimizeResult):
        if self._iteration % 15 == 0:
            print("\n niter |  neval | objective")
        self._iteration += 1
        print(f"{self._iteration:6d} | {len(self._results):6d} | {intermediate_result.fun:.3e}")

    def _tojson(self, name: str, data: dict):
        with open(self._path / f"{name}.json", "w") as f:
            json.dump(data, f, indent=4)


oo = OuterOptimization(run_iesopt, method="Nelder-Mead")
oo.add_decision("invest_gas", cost=500, lb=0, ub=5, initial_guess=2.5)
oo.add_decision("invest_storage", cost=100, lb=0, ub=5, initial_guess=2.5)
oo.add_decision("invest_pipeline", cost=1000, lb=0, ub=5, initial_guess=2.5)
oo.minimize()
