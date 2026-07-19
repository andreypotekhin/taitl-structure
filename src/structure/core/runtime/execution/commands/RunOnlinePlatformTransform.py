from structure.platform.api.v1.ExecutionRequest import ExecutionRequest


class RunOnlinePlatformTransform:
    def __call__(self, invocation, payload: object, *, session):
        return self._executor(payload).execute(
            ExecutionRequest(payload=payload, runtime=session, invocation=invocation, mode="online")
        )

    def _executor(self, payload: object):
        target = getattr(getattr(payload, "backend", None), "name", None)
        if not isinstance(target, str):
            raise ValueError("PLATFORM-E2708: Online execution requires a platform-owned payload.")
        from structure.core.platforms.api.Platform import Platform

        executor = Platform.registry().select(target).api.executor
        if executor is None:
            raise ValueError(f"PLATFORM-E2709: Platform {target!r} does not provide execution.")
        return executor
