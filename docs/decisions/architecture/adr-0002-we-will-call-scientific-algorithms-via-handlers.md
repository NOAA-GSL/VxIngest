# We will call scientific algorithms via handlers

Date: 2026-04-22

### Status

Accepted

### Context

We're using a scientific library (MetPy) to provide a number of meteorological calculations. [Their v1.7.0 release](https://github.com/Unidata/metpy/releases/tag/v1.7.0) modified the way they were calculating relative humidity and caused some debate around how to handle the resulting data discontinuity in our database.

### Decision

We will make sure scientific algorithms are called by VxIngest through handler functions.

This way the functions can be swapped out as needed. We also want to wrap the handler functions in unit tests to ensure our data doesn't change from version to version. We don't need to directly test libraries. This is already being done. But it's good to call out as a desired way of doing things.

### Consequences

#### Positive

- Isolating library calls in handler functions will make it easier to update & swap out scientific algorithms
- Establishing unit tests around these handler functions will help us detect if algorithm behavior changes

#### Neutral

- 

#### Negative

- There will be another layer of indirection between VxIngest & the algorithm calls.

