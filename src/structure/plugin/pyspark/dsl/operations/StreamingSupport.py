from structure.plugin.api.v1.model.StreamingSupport import StreamingSupport

# Streaming compatibility describes PySpark operation support.  Keep the
# shared contract type for reports while exposing its canonical bundled-plugin
# import path alongside the other PySpark operation concepts.
StreamingSupport.__module__ = __name__
