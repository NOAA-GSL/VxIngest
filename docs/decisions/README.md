# Decisions

This part of our documentation records decisions we have made for the VxIngest project. We've chosen to:

- Use the Architecture/Any Decision Record format for this purpose
- Make a distinction between "architecture" and "scientific" decision records
  - Architecture decisions are decisions focused on software & infrastructure patterns & use.
  - Scientific decisions are decisions focused on how to calculate & handle data

This is largely inspired by the work done in the beta.weather.gov rewrite. You can [see their usage of ADRs in their GitHub repo here](https://github.com/weather-gov/weather.gov/tree/main/docs/architecture/decisions)

## Purpose

Decision Records are intended to be a useful record for current and new project members. They aren't intended to capture every trivial decision we make as that would quickly become noisy. Decision Records should capture why the team made certain significant decisions. Think of the audience as being yourself two or more years from now, a new project member with no context on the project trying to come up to speed, or a scientist trying to understand how we verify certain values.

Decision records are supposed to be lightweight, so should be fairly quick to fill out. (However, the discussion prior to recording the decision may be more involved)

## Format

The format we will follow is largely [described by Michael Nygard in this article](http://thinkrelevance.com/blog/2011/11/15/documenting-architecture-decisions) and is referenced in the issue template.

**Title** The documents have concise titles that reflect the decision and start with "We will ...". For example, "We will record scientific decisions"

**Status** Either `Accepted` or `Superseded`. If `Superseded`, reference its replacement like so: `Superseded by [ADR 0000](adr-0000-the-adr-title.md)`

**Context** A statement of facts describing the motivating factors behind the decision: social, political, technical, scientific, etc. Why does this decision need to be made?

**Decision** What was decided? This should be an active voice statement. For example, "We will ..."

**Consequences** This section describes the resulting context, after applying the decision. All consequences should be listed here, not just the "positive" ones. A particular decision may have positive, negative, and neutral consequences, but all of them affect the team and project in the future.

**Filename** Decision records will be stored in Markdown files. The fileanames will be lower-case copies of the titles, solely with ASCII characters, and with spaces/underscores swapped for dashes (`-`). The title is prefaced by `adr-XXXX-` or `sdr-XXXX-` where `XXXX` is the ADR/SDR number. For example, `adr-0001-we-will-record-scientific-decisions.md`.

## Process

To open an ADR or SDR, you will need to:

1. First, make a significant decision as a team. We will most likely have discussed the core of the decision before the decision record is made.
2. Open a new issue and choose the appropriate ADR or SDR template.
3. Fill out the issue template to record the decision.
4. Ask for feed back in the issue. You can use `@` mentions to loop people in to the conversation.
5. Incorporate feedback by editing the ADR/SDR issue body until the decision is well captured.
6. Once the decision has been made, there are two routes:

    - Reject the decision - if we decide not to move forward with the decision, close the issue.
    - Accept the decision - if we decide to accept the decision, add the appropriate `ADR/SDR: accepted` label and close the issue

7. A PR with the Decision Record will be automatically created. Review the generated Markdown for sanity and merge it promptly.
8. If a decision record supersedes another, include a link to the superseded decision in the Decision Record's `Context` section. The old/superseded decision record's `Status` section will need to be manually updated in the PR to link to the new decision record. Use a relative markdown link like `Supersedes [ADR 0000](adr-0000-superseded-adr-file.md)`, instead of a GitHub URL to do the linking.
