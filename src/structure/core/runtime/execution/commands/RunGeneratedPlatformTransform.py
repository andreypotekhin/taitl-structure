from structure.platform.api.v1.model import ExecutionRequest


class RunGeneratedPlatformTransform:
    def __call__(self, invocation, payload: object, *, session, semantic_fingerprint: str | None = None):
        return self._executor(payload).execute(
            ExecutionRequest(
                payload=payload,
                runtime=session,
                invocation=invocation,
                mode="generated",
                semantic_fingerprint=semantic_fingerprint,
            )
        )

    def _executor(self, payload: object):
        target = getattr(getattr(payload, "backend", None), "name", None)
        if not isinstance(target, str):
            raise ValueError("PLATFORM-E2708: Generated execution requires a platform-owned payload.")
        from structure.core.platforms.api.Platform import Platform

        executor = Platform.registry().select(target).api.executor
        if executor is None:
            raise ValueError(f"PLATFORM-E2709: Platform {target!r} does not provide execution.")
        return executor
