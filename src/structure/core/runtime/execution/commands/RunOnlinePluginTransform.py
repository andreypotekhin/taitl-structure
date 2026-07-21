from structure.plugin.api.v1.model import ExecutionRequest


class RunOnlinePluginTransform:
    def __call__(self, invocation, payload: object, *, session):
        return self._executor(payload).execute(
            ExecutionRequest(payload=payload, runtime=session, invocation=invocation, mode="online")
        )

    def _executor(self, payload: object):
        target = getattr(getattr(payload, "backend", None), "name", None)
        if not isinstance(target, str):
            raise ValueError("PLUGIN-E2708: Online execution requires a plugin-owned payload.")
        from structure.core.plugins.api.Plugin import Plugin

        executor = Plugin.registry().select(target).api.executor
        if executor is None:
            raise ValueError(f"PLUGIN-E2709: Plugin {target!r} does not provide execution.")
        return executor
