from structure.app.target.capabilities.model.BackendCapabilityError import BackendCapabilityError
from structure.app.target.capabilities.model.BackendId import BackendId
from structure.app.target.capabilities.model.CapabilityDecision import CapabilityDecision
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.capabilities.model.GeneratedImports import GeneratedImports

DEFAULT_TARGET_PYSPARK = ">=3.5,<4.1"

SUPPORTED_TARGETS = frozenset(
    {
        ">=3.5,<4.1",
        ">=3.5,<4.0",
        ">=4.0,<4.1",
    }
)

V1_CAPABILITIES = frozenset(
    {
        ("backend", "ordinary_pyspark"),
        ("expression", "field_ref"),
        ("expression", "literal"),
        ("expression", "projection"),
        ("expression", "filter"),
        ("expression", "boolean_ops"),
        ("expression", "equality"),
        ("expression", "null_safe_equality"),
        ("expression", "cast"),
        ("expression", "standard_helper_call"),
        ("join", "join_one"),
        ("join", "exists"),
        ("join", "not_exists"),
        ("join", "left_join"),
        ("join", "inner_join"),
        ("join", "left_semi_join"),
        ("join", "left_anti_join"),
        ("join", "composite_equi_join"),
        ("join", "broadcast_hint"),
        ("validation", "schema_only_validation"),
        ("validation", "strict_projection"),
        ("validation", "allow_extra_projection"),
        ("streaming", "row_local_projection"),
        ("streaming", "row_local_filter"),
        ("streaming", "stream_static_left_join"),
        ("streaming", "stream_static_inner_join"),
        ("imports", "generated_pyspark_imports"),
    }
)


class PySparkCapabilities:

    def __init__(
        self,
        *,
        target_pyspark: str = DEFAULT_TARGET_PYSPARK,
        supported: frozenset[tuple[str, str]] = V1_CAPABILITIES,
    ) -> None:
        self.id = BackendId(name="pyspark", target=target_pyspark, family="ordinary_pyspark")
        self.supported = supported
        self._imports = GeneratedImports()

    def imports(self) -> GeneratedImports:
        return self._imports

    def supports(self, requirement: CapabilityRequirement) -> CapabilityDecision:
        if self.id.target not in SUPPORTED_TARGETS:
            return CapabilityDecision.unsupported_capability(
                backend=self.id,
                requirement=requirement,
                rationale="No static PySpark capability profile exists for the configured target range.",
                use=f"Set target_pyspark = {DEFAULT_TARGET_PYSPARK!r}.",
                required_target=DEFAULT_TARGET_PYSPARK,
            )

        if requirement.key() in self.supported:
            return CapabilityDecision.ok(backend=self.id, requirement=requirement)

        return CapabilityDecision.unsupported_capability(
            backend=self.id,
            requirement=requirement,
            rationale=(
                "The feature is not part of the v1 PySpark capability profile. Keeping it unsupported prevents "
                "silent fallback to opaque Spark or Python behavior."
            ),
            use="Use a supported v1 Structure operation or an explicit hook, or wait for the feature's specification.",
        )

    def require(self, requirement: CapabilityRequirement) -> CapabilityDecision:
        decision = self.supports(requirement)
        if not decision.supported:
            raise BackendCapabilityError(decision)
        return decision
