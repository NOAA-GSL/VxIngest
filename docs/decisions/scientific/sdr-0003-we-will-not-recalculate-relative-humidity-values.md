# We will not recalculate relative humidity values

Date: 2026-02-12

### Status

Accepted

### Context

We rely on Unidata's MetPy library for a number of meteorological calculations. The [v1.7.0 release of MetPy](https://github.com/Unidata/MetPy/releases/tag/v1.7.0) introduced a change in its calculation of [saturation vapor pressure](https://unidata.github.io/MetPy/v1.7/api/generated/metpy.calc.saturation_vapor_pressure.html) that affects our relative humidity calculations. We noticed this change due to our unit & integration tests. We need to decide how to handle this change and other algorithmic changes that would affect long term analysis.

### Decision

We decided:
* The change for relative humidity calculations was small, (relative to the significant figures for RH) so there's no need to recalculate our data based on the new equation
* For now, we won't adopt but will evaluate the new "`auto`" mixing methodology (switching between `ice` and `liquid` when temp > 0 Celsius) they use to calculate relative humidity.  

### Consequences

#### Positive

- The new relative humidity calculation is more accurate, and can support different mixing regimes

#### Neutral

- We won't recalculate previously calculated relative humidity values
- The new relative humidity mixing regimes may not be appropriate in all situations, so for now, we will evaluate them.

#### Negative

- There will be a slight change in the relative humidity values the ingest produces when we deploy the newer version. It's unlikely, but these differences could show up in modeler's analyses if the analysis includes data from before and after the change.

### Additional Info

#### Summary of Methodology Changes

The MetPy developers have decided to move away from using the "standard" version of the Clausius-Clapeyron equation found in the Bolton Dynamic Meteorology textbook, and switch to using the one described in https://rmets.onlinelibrary.wiley.com/doi/full/10.1002/qj.3899. They have also introduced functionality that tries to figure out what percentage of the water in the air is ice crystals, and then does a weighted average of Clausius-Clapeyron for liquid/vapor and the Clausius-Clapeyron for ice. Both of these changes were introduced to more accurately calculate the Lifted Condensation Level (LCL). Code changes can be viewed here: https://github.com/Unidata/MetPy/pull/3726
