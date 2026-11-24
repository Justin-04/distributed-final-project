# Lazy exports: importing any tools submodule (e.g. tools.extraction in an ingest
# worker) must NOT load the retrieval stack (BM25 index build + cross-encoder model),
# which takes ~20 s and several hundred MB. retrieval is only loaded when used.

__all__ = [
    "query_dji_manual_vector_db",
    "lookup_dji_error_code_db",
]


def __getattr__(name):
    if name == "query_dji_manual_vector_db":
        from .retrieval import retrieve
        return retrieve
    if name == "lookup_dji_error_code_db":
        from .error_code_tool import lookup_dji_error_code_db
        return lookup_dji_error_code_db
    raise AttributeError(name)
