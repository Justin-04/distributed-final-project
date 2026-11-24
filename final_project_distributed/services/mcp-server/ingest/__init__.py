"""
Distributed, event-driven ingestion pipeline.

    producer (MCP server)  ──XADD──▶  ingest:jobs   ──▶ splitter worker(s)
    splitter               ──XADD──▶  ingest:pages  ──▶ page workers (consumer group, N replicas)
    last page worker       ──PUBLISH─▶ ingest:events ──▶ every MCP server / Agent A replica

Redis is the coordination plane (streams, job state, rate limits, image claims);
Qdrant receives the vectors; S3 holds the raw PDF and the extracted images.
"""
