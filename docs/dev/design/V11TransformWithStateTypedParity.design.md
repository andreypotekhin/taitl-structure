# Typed state parity for row `transform_with_state`

## Status

Accepted design for a future breaking update to the typed row processor API. This document does not describe the current
public API as implemented. Row `transform_with_state(...)` currently supports one typed `ValueState` passed as a
callback argument, plus an opaque native PySpark processor path. Implementation work belongs to a separate ExecPlan.

## Purpose

PySpark's `StatefulProcessor` API lets a processor declare named state handles on its instance and use them from input,
timer, and initial-state callbacks. Structure currently models one unnamed value state and passes it into each typed
callback. This design replaces that narrow model with annotated named attributes for value, list, and map state, while
keeping the processor body as ordinary Python.

Structure continues to own the adapter boundary. It validates state names and schemas, constructs PySpark handles in
`init`, wraps values as Structure `Schema` objects, fingerprints declarations for explain and generated artifacts,
and retains typed execution in the shared operation recipe. It does not translate callback Python into Spark SQL.

## Public API

The typed base has three schema parameters: input, grouping key, and output. The state schema is declared at each state
attribute, where it can be named and independently typed.

    @state_processor
    class AccountProcessor(StateProcessor[Event, AccountKey, AccountOutput]):
        total: ValueState[Total]
        recent_events: ListState[Event]
        summaries: MapState[SummaryKey, Summary] = map_state(
            name="summaries",
            ttl=timedelta(hours=24),
        )

        def on_rows(
            self,
            key: AccountKey,
            rows: Iterator[Event],
            timers: TimerContext,
        ) -> Iterator[AccountOutput]:
            current = self.total.get()
            ...

        def on_timer(
            self,
            key: AccountKey,
            timer: Timer,
            timers: TimerContext,
        ) -> Iterator[AccountOutput]:
            ...

`@state_processor` remains parameterless. State declarations are class annotations rather than decorator arguments or
`transform_with_state(...)` parameters. A declaration factory is needed only when the default attribute name or no-TTL
policy is insufficient. The factory returns immutable declaration metadata at class-definition time; it is not a live
Spark handle.

The operation retains relation-bound settings at its call site:

    transform_with_state(
        key=(event.account_id, event.region),
        processor=AccountProcessor,
        output_mode="Update",
        time_mode="EventTime",
        event_time_column="event_time",
        initial_state=self.initial,
    )

`key` binds the processor to fields of the current input relation. One-field keys take one field expression. Composite
keys take a tuple whose order matches the declared fields in the processor's Key Schema. A mapping is not accepted;
the tuple and Schema declaration together are the only order and field-type contract.

## State declarations and handles

Annotation-only declarations use the Python attribute name as the persisted Spark state name and have no TTL:

    total: ValueState[Total]

The optional factories are `value_state(...)`, `list_state(...)`, and `map_state(...)`. They accept `name: str | None`
and `ttl: datetime.timedelta | None`. When `name` is omitted, the attribute name remains the persisted name. An
explicit name permits a Python attribute rename while retaining the Spark state identity. Names are nonempty and
unique within one processor.

Each state kind has one typed wrapper over the matching PySpark 4.1 handle.

- `ValueState[ValueSchema]` provides `exists()`, `get() -> ValueSchema | None`, `update(value)`, and `clear()`.
- `ListState[ValueSchema]` provides `exists()`, lazy `get()`, `append_value(value)`, `append_list(values)`,
  `put(values)`, and `clear()`.
- `MapState[KeySchema, ValueSchema]` provides `exists()`, `contains_key(key)`, `get_value(key)`,
  `update_value(key, value)`, lazy `iterator()`, `keys()`, `values()`, `remove_key(key)`, and `clear()`.

Every write validates the supplied Structure Schema before calling Spark. Every read converts Spark rows into Structure
Schema instances using declared field names and order. List and map traversal stays lazy so a large state collection is
not copied into driver memory. Map keys use a Structure Schema and Spark's declared user-key schema. The wrapper does
not add ordering guarantees that Spark does not provide.

TTL is expressed with `datetime.timedelta`, not a bare integer whose unit could be misread. A factory converts it to an
exact positive integer number of milliseconds. It rejects zero or negative durations, any duration containing
sub-millisecond precision, and values outside Spark's accepted integer range. `None` means no expiry. TTL has meaning
only in time modes supported by the pinned runtime; compilation rejects incompatible combinations before lowering.

## Processor lifecycle and callback rules

Structure's generated adapter owns PySpark `StatefulProcessor.init(handle)`. It creates every declared Spark handle
there and assigns typed wrappers to the processor instance before invoking any user callback. State attributes are
therefore unavailable in a user-defined `__init__`; processor constructors may initialize ordinary configuration only.
Users must not define the Spark `init` method on a typed Structure processor because doing so would bypass declared
handle creation.

`on_rows(key, rows, timers)`, `on_timer(key, timer, timers)`, and optional `on_initial_state(key, initial, timers)` are
ordinary Python methods. Input and output rows use their Structure Schema classes. The timer argument is the current
Structure timer value. State access is limited to the current callback and key. Users must not pass a handle to another
thread or retain it in global state. Spark may retry work, so side effects in callback bodies remain the user's
responsibility and are not made transactional by Structure.

The typed contract does not expose `close` as a reliable cleanup point. Spark cannot guarantee cleanup after worker or
process failure. Users requiring resource lifecycle code or another supported PySpark processor hook must use the
external processor path.

## Typed initial state

Typed initial state stays at the operation call because it binds the processor to a separate relation. A processor opts
in by defining a concrete callback:

    def on_initial_state(
        self,
        key: AccountKey,
        initial: InitialAccount,
        timers: TimerContext,
    ) -> None:
        self.total.update(Total(value=initial.total))

The `initial` parameter annotation is a concrete Structure Schema, not a new generic parameter. All state wrappers are
available before this callback runs. Compilation checks that the callback key annotation matches the processor Key
Schema, `initial_state=` names a declared transform input, the relation has the declared initial-row schema, and its
key fields match the operation's grouping key. A callback without an initial relation and an initial relation without
the callback are compile errors.

## State identity, fingerprints, and checkpoint changes

Persisted identity is the explicit `name=` or, by default, the Python attribute name. A typed state declaration's
traceability fingerprint includes its attribute and persisted names, kind, key and value schemas, and TTL. The
fingerprint makes generated artifacts and explain output distinguish state-contract changes; it does not migrate a
Spark checkpoint.

Renaming a Python attribute can retain state identity by supplying the existing persisted name explicitly. Changing a
persisted name, state kind, key or value Schema, or TTL is checkpoint-sensitive. Structure does not own the checkpoint
path and does not create migration files beside it. Spark remains responsible for enforcing state-store compatibility.
Until a pinned runtime behavior and migration contract are separately documented, users must start a new checkpoint
after changing a persisted declaration.

No automatic state evolution, dual state names, or compatibility lookup is part of this design. A future migration
feature would need an explicit old-to-new declaration, a safe rollback model, and live restart evidence before it can
relax this rule.

## External native processors

`external_state_processor(...)` remains the escape hatch for Python code or PySpark behavior outside the typed model.
It binds the native processor class to Structure input, key, output, and informational state Schema declarations.
Structure passes the processor to PySpark without inspecting its callback bodies or state declarations. State kinds,
names, TTL, timers, cleanup, direct Spark calls, Python libraries, side effects, and checkpoint compatibility remain
opaque and caller-owned.

The external boundary still participates in the Structure plan: the compiler validates relation schemas and target
capability, generated modules retain the processor's importable reference, explain and traceability label the
processor opaque, and online and generated paths call the same runtime helper. Callers deploy the processor module and
its dependencies to Spark workers. This path is for valid Python constructs the Structure compiler does not support as
well as Spark processor features not yet represented by typed declarations.

## Breaking change and migration

Backward compatibility with the current typed processor is not required. The follow-on implementation changes
`StateProcessor[Input, Key, State, Output]` to `StateProcessor[Input, Key, Output]` and removes the `state` argument
from `on_rows` and `on_timer` in one release. It does not interpret a Schema in the old generic position, add a
`StateModel` alternative, accept state schemas at `@state_processor`, or support both callback shapes.

An existing Structure processor moves its old state Schema onto an attribute. For example,
`StateProcessor[Event, Key, Total, Output]` becomes `StateProcessor[Event, Key, Output]`, the callback's
`state: ValueState[Total]` parameter is removed, and its reads and writes become `self.total.get()` and
`self.total.update(...)`. Existing native PySpark code that creates
`self.total = handle.getValueState("total", schema)` in `init` can declare `total: ValueState[Total]` instead when the
typed wrapper covers its operations. Processor Python logic otherwise remains ordinary Python.

No deprecation stage or automatic checkpoint migration is required. The migration guide must call out checkpoint
compatibility separately from Python source compatibility.

## Implementation order and validation

The follow-on ExecPlan implements the change in this order. First replace the four-generic typed processor with the
three-generic model and implement named `ValueState` declarations; this proves class annotation discovery, wrapper
injection, generated imports, and the source break. Next add `ListState` and `MapState` with separate conversion and
online/generated behavior tests. Then add TTL conversion and live expiry evidence. Add typed initial state only after
the callback and relation schema rules have compiler tests and pinned live evidence. Finally record checkpoint
fingerprints and diagnostics while keeping automatic migration out of scope.

The compiler tests cover generic resolution, annotation validation, inherited declarations, duplicate names, reserved
attributes, invalid TTL, state-kind conversion, key order, optional initial-state signatures, generated references,
explain, and opaque external processors. The ordinary PySpark 4.1 integration lane proves each state kind online and
generated, restart with an unchanged declaration, timer callback access, TTL expiry, and typed initial-state
population. A changed checkpoint declaration is tested for an actionable Spark failure or is explicitly documented as
Spark-controlled if Spark does not expose a stable error.

Row mode admission, composite key support, and event-time timers are governed by
`docs/dev/planning/past/P10062603.V11-transform-with-state-admission-and-typed-parity.plan.md`. The separate
`transform_with_state_in_pandas(...)` contract retains its own implementation plan and evidence. This design does not
transfer row evidence to the Pandas API.

## Decision record

- Decision: Use named state handles declared as processor attributes, with annotations as the default declaration
  syntax and factories only for explicit name or TTL metadata.
  Rationale: This matches native PySpark processor examples and keeps state declarations together without repeating
  them at the transform call.
- Decision: Use three processor Schema generics and do not retain or reinterpret the old four-generic shape.
  Rationale: The state Schema belongs to each named handle. Backward compatibility and polymorphic generic dispatch
  are not requirements.
- Decision: Keep external native processors as an opaque, first-class boundary.
  Rationale: Structure supports typed symbolic contracts while allowing ordinary Python constructs and PySpark
  features that its compiler does not model.
- Decision: Do not promise automatic checkpoint migration or typed cleanup through `close`.
  Rationale: Structure does not own the query checkpoint or Spark worker failure lifecycle.

Revision note (2026-10-06): Recorded the selected attribute-based API and breaking migration direction while
implementing the row admission and evidence plan.
