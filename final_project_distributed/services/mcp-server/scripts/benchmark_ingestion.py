"""
Ingestion benchmark: sequential baseline vs distributed workers, on the same PDF.

    # sequential baseline (old code path, runs in this process)
    python scripts/benchmark_ingestion.py path/to/file.pdf --mode sequential --cleanup

    # distributed (needs the MCP server on :8002 and N workers running)
    python scripts/benchmark_ingestion.py path/to/file.pdf --mode distributed --cleanup

    # make a small synthetic test manual
    python scripts/benchmark_ingestion.py --make-test-pdf test.pdf --pages 12 --images 8

Run from services/mcp-server. --cleanup deletes the document (chunks + S3 images)
afterwards, so runs do not influence each other.
"""

import os
import sys
import json
import time
import base64
import argparse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv  # noqa: E402

load_dotenv()

MCP = os.getenv("MCP_SERVER_URL", "http://localhost:8002")


def _http(method: str, path: str, body: dict = None) -> dict:
    req = urllib.request.Request(
        MCP + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=600) as r:
        return json.loads(r.read())


def run_sequential(pdf: str, name: str, model: str) -> dict:
    from tools.documents_tool import upload_and_ingest
    with open(pdf, "rb") as f:
        data = f.read()
    t0 = time.perf_counter()
    res = upload_and_ingest(data, name, model, caption_images=True)
    res["wall_seconds"] = round(time.perf_counter() - t0, 2)
    t = res.get("timings", {})
    # ingestion proper, excluding the BM25 rebuild that this path runs inline
    res["ingest_seconds"] = round(sum(t.get(k, 0) for k in
                                      ("extract_caption_s3_seconds", "chunking_seconds", "embed_upsert_seconds")), 2)
    return res


def run_distributed(pdf: str, name: str, model: str) -> dict:
    with open(pdf, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    t0 = time.perf_counter()
    job = _http("POST", "/api/v1/ingest/jobs", {"file_bytes_b64": b64, "filename": name, "drone_model": model})
    accepted = time.perf_counter() - t0
    print(f"  202 Accepted in {accepted:.2f}s — job {job['job_id']}")
    last = None
    while True:
        job = _http("GET", f"/api/v1/ingest/jobs/{job['job_id']}")
        line = f"  {job['status']:<22} {job['pages_finished']}/{job['total_pages']} pages  {len(job['workers'])} worker(s)"
        if line != last:
            print(line)
            last = line
        if job["status"] in ("completed", "completed_with_errors", "failed", "cancelled"):
            break
        time.sleep(0.5)
    job["wall_seconds"] = round(time.perf_counter() - t0, 2)
    job["accepted_seconds"] = round(accepted, 2)
    return job


def cleanup(name: str):
    from tools.documents_tool import delete_document
    r = delete_document(name)
    print(f"  cleanup: {r.get('status')} ({r.get('chunks_removed', 0)} chunks, {r.get('images_deleted', 0)} images)")


def make_test_pdf(path: str, pages: int, images: int):
    """Synthetic drone manual: text on every page, coloured diagrams on some pages."""
    import fitz
    doc = fitz.open()
    image_pages = set(range(1, pages, max(1, pages // max(1, images))))
    n_img = 0
    for p in range(pages):
        page = doc.new_page()
        text = (f"Benchmark Drone X1 User Manual - Page {p + 1}\n\n"
                f"Section {p + 1}: Battery, flight modes and safety.\n"
                "The intelligent flight battery has a capacity of 2590 mAh at 7.32 V. "
                "Charge the battery fully before each flight. In Normal mode the maximum "
                "horizontal speed is 12 m/s; in Sport mode it reaches 16 m/s. Return to Home "
                "is triggered automatically when the battery level is critically low. "
                "Do not fly near airports or in restricted GEO zones. ") * 3
        page.insert_textbox(fitz.Rect(50, 50, 550, 500), text, fontsize=10)
        if p in image_pages and n_img < images:
            pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 320, 200), 0)
            pix.clear_with(60 + (n_img * 23) % 180)  # distinct grey per image → distinct xref
            page.insert_image(fitz.Rect(150, 520, 470, 720), pixmap=pix)
            page.insert_text((150, 740), f"Figure {n_img + 1}: aircraft status LED diagram", fontsize=9)
            n_img += 1
    doc.save(path)
    print(f"Created {path}: {pages} pages, {n_img} images")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", nargs="?")
    ap.add_argument("--mode", choices=["sequential", "distributed"], default="distributed")
    ap.add_argument("--name", help="source name to store (default: file name)")
    ap.add_argument("--model", default="DJI Benchmark Test")
    ap.add_argument("--cleanup", action="store_true")
    ap.add_argument("--make-test-pdf")
    ap.add_argument("--pages", type=int, default=12)
    ap.add_argument("--images", type=int, default=8)
    args = ap.parse_args()

    if args.make_test_pdf:
        make_test_pdf(args.make_test_pdf, args.pages, args.images)
        return

    name = args.name or os.path.basename(args.pdf)
    print(f"=== {args.mode} ingestion of {name}")
    res = run_sequential(args.pdf, name, args.model) if args.mode == "sequential" \
        else run_distributed(args.pdf, name, args.model)
    print(json.dumps(res, indent=2, default=str))
    if args.cleanup:
        cleanup(name)


if __name__ == "__main__":
    main()
