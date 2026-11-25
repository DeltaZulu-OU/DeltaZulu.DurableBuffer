# DeltaZulu.DurableBuffer — documentation

Estate-wide architectural policy, Constraints, the current target and planning are maintained in [DeltaZulu-OU/docs](https://github.com/DeltaZulu-OU/docs). Local documents describe source, interfaces and historical reasoning; they do not override Accepted Decisions.

| Looking for | Go to |
|---|---|
| Current target | [NRT pipeline](https://github.com/DeltaZulu-OU/docs/blob/master/architecture/NRT-PIPELINE.md) |
| Governing Decisions | [Decision index](https://github.com/DeltaZulu-OU/docs/blob/master/architecture/GOVERNING-DECISIONS.md) |
| Constraints and qualifications | [Constraints](https://github.com/DeltaZulu-OU/docs/tree/master/constraints) |
| Active work | [Consolidated roadmap](https://github.com/DeltaZulu-OU/docs/blob/master/roadmaps/CONSISTENCY-CORRECTNESS-ROADMAP.md) (C14) |
| Historical architecture | [Pipeline import](https://github.com/DeltaZulu-OU/docs/blob/master/architecture/PIPELINE.md) with [errata](https://github.com/DeltaZulu-OU/docs/blob/master/architecture/PIPELINE-ERRATA.md) |
| Historical ADRs | [Archive](https://github.com/DeltaZulu-OU/docs/tree/master/archive) |
| Dated verification evidence | [Reports](https://github.com/DeltaZulu-OU/docs/tree/master/reports) |

## Local scope

This is a payload-opaque durable edge queue, distinct from LocalStream's retained multi-consumer log. It is optional at service edges under DEC-0033. Historical release/version observations are not current package proof; C14 covers compatibility and publishing evidence.

Only tested behavior is verified; a target architecture or historical report is not proof of current runtime conformance.
