# We will use snake_case for Couchbase document keys

Date: 2026-04-22

### Status

Accepted

### Context

We have a variety of different naming styles in our Couchbase documents. A quick sample is showing `camelCase`, `UpperCamelCase`, `snake_case`, `UPPER_SNAKE_CASE`, and `kebab-case`. There are also a few keys with spacing. (E.g. `Surface Pressure` in `DD:V01:METAR:obs:*` docs) We should standardize on one style for consistency, and so we don't have to think about naming. 

### Decision

We will utilize`snake_case` for keys in new Couchbase document schemas, and update old document schemas when possible. 

### Consequences

#### Positive

- We will have consistent naming in our Couchbase documents
- Python prefers `snake_case` naming, so `snake_case` JSON will be a natural fit in our main data processing language

#### Neutral

- We may need to have "display names" for JSON keys so that, for example, `relativeHumidity` is displayed as `Relative Humidity` in a UI. This is an issue with the current document schemas as well. However, if the naming is consistent, we can do this algorithmically.
- Go & JavaScript have preferences for `camelCase`. We will need to modify linter rules accordingly. This is easy to do in Go. I don't believe our JavaScript linter checks JSON keys.

#### Negative

- We already have documents in Couchbase with a variety of naming styles, we will want to update those when possible to utilize `snake_case`

