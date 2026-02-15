import csv
import sys
import time
import random
import shutil
import subprocess
import hashlib
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional

import requests

# --- Config ---
CSV_PATH = Path("sound_list.csv")
OUT_DIR = Path("output")
ARCHIVE_DIR = Path("output_archive")

# Concurrency
MAX_WORKERS = 3  # run up to 3 in parallel

# Networking / retry
GENERATE_URL = "https://glados.c-net.org/generate"
MAX_RETRIES = 30
BASE_BACKOFF_SECONDS = 1.0     # initial backoff
BACKOFF_MULTIPLIER = 2.0       # exponential factor
MAX_BACKOFF_SECONDS = 30.0     # cap to avoid very long sleeps
TIMEOUT_SECONDS = 60

# ffmpeg encoding quality
VORBIS_QSCALE = "5"  # 0–10 (higher = better quality, larger files)


def normalize_text(text: str) -> str:
    """
    Matches the original normalization used before hashing & sending to the API.
    """
    return (
        text.replace("…", "...")
            .replace("’", "'")
            .replace("‑", "-")
            .replace("—", "- ")
            .strip()
    )


def parse_index_field(field: str) -> Optional[int]:
    """
    Parses the first CSV column which can be:
      - a plain number: "0", "12", "003"
      - a filename-like: "0.ogg", "12.wav", "003.OGG"
    Returns an integer index or None if it cannot be parsed.
    """
    if field is None:
        return None
    s = field.strip()
    # Prefer a leading number; allow optional extension
    m = re.match(r"^\s*(\d+)(?:\.[A-Za-z0-9]+)?\s*$", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def load_items_from_csv(path: Path) -> List[Tuple[int, str]]:
    """
    Reads semicolon-separated CSV with at least two columns:
      col 1: index or filename (e.g., '0' or '0.ogg')
      col 2: text
    Returns a list of (index, normalized_text).
    Skips malformed rows with a warning to stderr.
    """
    items: List[Tuple[int, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for row_num, row in enumerate(reader, start=1):
            if not row:
                continue
            if len(row) < 2:
                print(
                    f"[CSV] Row {row_num}: expected 2 columns; got {len(row)}. Skipped.", file=sys.stderr)
                continue

            idx_raw, text_raw = row[0], row[1]
            idx = parse_index_field(idx_raw)
            text = normalize_text(text_raw)

            if idx is None:
                print(
                    f"[CSV] Row {row_num}: could not parse index from '{idx_raw}'. Skipped.", file=sys.stderr)
                continue
            if not text:
                print(
                    f"[CSV] Row {row_num}: empty text. Skipped.", file=sys.stderr)
                continue

            items.append((idx, text))
    return items


def text_hash_md5(text: str) -> str:
    """
    MD5 of the normalized text. This ensures filename changes when the voiceline changes.
    """
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


def _sleep_with_jitter(seconds: float) -> None:
    # Add 0–200ms jitter to reduce thundering herd on retries
    time.sleep(seconds + random.random() * 0.2)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _move_to_archive(p: Path) -> Path:
    """
    Move a file p into ARCHIVE_DIR. If a file with same name exists in archive,
    append a timestamp suffix before the extension.
    Returns the final archived path.
    """
    _ensure_dir(ARCHIVE_DIR)
    target = ARCHIVE_DIR / p.name
    if target.exists():
        # append a timestamp to avoid clobbering
        ts = int(time.time())
        if p.suffix:
            target = ARCHIVE_DIR / f"{p.stem}.{ts}{p.suffix}"
        else:
            target = ARCHIVE_DIR / f"{p.name}.{ts}"
    p.rename(target)
    return target


def archive_mismatched_outputs(index: int, keep_md5: str) -> Tuple[int, int]:
    """
    For a given index, move any existing output files (ogg/wav) that have a different
    hash than keep_md5 into ARCHIVE_DIR.

    Returns (num_archived, num_checked).
    """
    num_archived = 0
    num_checked = 0

    # Check both ogg and wav for this index
    for ext in (".ogg", ".wav"):
        for candidate in OUT_DIR.glob(f"{index}-*{ext}"):
            num_checked += 1
            name = candidate.name  # e.g., "0-<hash>.ogg"
            try:
                # Extract hash between first '-' and extension
                dash_pos = name.find("-")
                dot_pos = name.rfind(".")
                if dash_pos == -1 or dot_pos == -1 or dot_pos <= dash_pos + 1:
                    _move_to_archive(candidate)
                    num_archived += 1
                    continue

                cand_hash = name[dash_pos + 1:dot_pos]
                if cand_hash != keep_md5:
                    _move_to_archive(candidate)
                    num_archived += 1
            except Exception:
                _move_to_archive(candidate)
                num_archived += 1

    return num_archived, num_checked


def download_wav(text: str, wav_path: Path) -> None:
    """
    Downloads WAV using GLaDOS endpoint with robust retries.
    Creates parent directories as needed.
    """
    wav_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                GENERATE_URL,
                params={"text": text},
                stream=True,
                timeout=TIMEOUT_SECONDS,
            )
            r.raise_for_status()

            with wav_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            transient = status in (429, 500, 502, 503, 504)
            if attempt >= MAX_RETRIES or not transient:
                raise
        except requests.RequestException:
            if attempt >= MAX_RETRIES:
                raise

        backoff = min(
            MAX_BACKOFF_SECONDS,
            BASE_BACKOFF_SECONDS * (BACKOFF_MULTIPLIER ** (attempt - 1)),
        )
        _sleep_with_jitter(backoff)


def convert_wav_to_ogg(wav_path: Path, ogg_path: Path, gain_db: float = 8.0, limit_peak: float = 0.95) -> None:
    """
    Converts WAV -> OGG (Vorbis) using ffmpeg, applies gain, and a peak limiter to avoid clipping.
    """
    ogg_path.parent.mkdir(parents=True, exist_ok=True)
    # Chain: volume -> limiter
    af = f"volume={gain_db}dB,alimiter=limit={limit_peak}"
    cmd = [
        "ffmpeg",
        "-y",
        "-i", str(wav_path),
        "-filter:a", af,
        "-codec:a", "libvorbis",
        "-qscale:a", VORBIS_QSCALE,
        str(ogg_path),
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.STDOUT)


def process_one(index: int, text: str, ffmpeg_ok: bool) -> Tuple[int, str, str]:
    """
    Worker task: download (if needed) and convert one line.
    Filenames include MD5 of text: {index}-{md5}.ogg
    Returns a tuple (index, status, message)
      - status: 'ok' or 'skip' or 'error'
      - message: details
    """
    md5 = text_hash_md5(text)
    base = f"{index}-{md5}"
    wav_path = OUT_DIR / f"{base}.wav"
    ogg_path = OUT_DIR / f"{base}.ogg"

    # Archive any mismatched outputs (same index, different hash) first
    archive_error = None
    try:
        archived, checked = archive_mismatched_outputs(index, md5)
        # (Optional) could log archived/checked if desired
    except Exception as e:
        archive_error = str(e)

    # If final OGG exists, skip
    if ogg_path.exists():
        msg = f"OGG exists: {ogg_path.name}"
        if archive_error:
            msg += f" (archive warn: {archive_error})"
        return index, "skip", msg

    # If WAV exists but OGG missing and ffmpeg available, convert only
    if wav_path.exists() and ffmpeg_ok:
        try:
            convert_wav_to_ogg(wav_path, ogg_path)
            wav_path.unlink(missing_ok=True)
            msg = f"Converted existing WAV -> {ogg_path.name}"
            if archive_error:
                msg += f" (archive warn: {archive_error})"
            return index, "ok", msg
        except subprocess.CalledProcessError as e:
            return index, "error", f"ffmpeg convert error: {e}"

    # Otherwise, download WAV
    try:
        download_wav(text, wav_path)
    except Exception as e:
        return index, "error", f"download error: {e}"

    # Convert to OGG (if ffmpeg). If not, keep WAV.
    if ffmpeg_ok:
        try:
            convert_wav_to_ogg(wav_path, ogg_path)
            wav_path.unlink(missing_ok=True)
            msg = f"Saved {ogg_path.name}"
            if archive_error:
                msg += f" (archive warn: {archive_error})"
            return index, "ok", msg
        except subprocess.CalledProcessError as e:
            return index, "error", f"ffmpeg convert error: {e}"
    else:
        msg = f"Saved WAV (no ffmpeg): {wav_path.name}"
        if archive_error:
            msg += f" (archive warn: {archive_error})"
        return index, "ok", msg


def main():
    items = load_items_from_csv(CSV_PATH)
    if not items:
        print(
            f"No valid rows found in {CSV_PATH}. Expected: first column index/filename; second column text.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Optional: detect duplicate indices (could cause races or archiving each other)
    seen = {}
    dups = {}
    for pos, (idx, _) in enumerate(items):
        if idx in seen:
            dups.setdefault(idx, []).append(pos)
        else:
            seen[idx] = pos

    if dups:
        dup_list = ", ".join(str(k) for k in sorted(dups.keys()))
        print(
            f"Warning: duplicate indices detected in CSV: {dup_list}. "
            f"The same index will produce files like {{index}}-{{md5}}.* and may archive older variants.",
            file=sys.stderr,
        )

    ffmpeg_ok = has_ffmpeg()
    if not ffmpeg_ok:
        print("Warning: ffmpeg not found. Files will be left as .wav", file=sys.stderr)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    jobs = items[:]  # list of (index, text) as parsed from CSV
    total = len(jobs)
    print(f"Starting {total} items with {MAX_WORKERS} parallel workers...")

    completed = 0
    errors = 0
    skipped = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(process_one, i, text, ffmpeg_ok)
                       for i, text in jobs]
            for fut in as_completed(futures):
                index, status, message = fut.result()
                if status == "ok":
                    completed += 1
                    print(f"[{index}] ✅ {message}")
                elif status == "skip":
                    skipped += 1
                    print(f"[{index}] ⏭️  {message}")
                else:
                    errors += 1
                    print(f"[{index}] ❌ {message}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Some tasks may still be running.",
              file=sys.stderr)
        sys.exit(130)

    print(f"\nDone. OK: {completed}, Skipped: {skipped}, Errors: {errors}")


if __name__ == "__main__":
    main()
