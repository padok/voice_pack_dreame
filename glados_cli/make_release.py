import hashlib
import re
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, List, Optional

# --- Config ---
OUT_DIR = Path("output")
ARCHIVE_PATH = Path("voice_pack.tar.gz")  # output archive at project root
README_PATH = Path("README.md")

# Regex to match hashed oggs such as "12-a0b1c2...ff.ogg"
OGG_PATTERN = re.compile(r"^(?P<index>\d+)-(?P<md5>[0-9a-fA-F]{32})\.ogg$")

# README replacement patterns (robust but specific to your shared README format)
# We will replace the content inside backticks or after labels.

RE_MD5_BLOCK = re.compile(
    r"(MD5 sum of the prepackaged\s*`voice_pack\.tar\.gz`:\s*\r?\n\s*`)([0-9a-fA-F]{32})(`)",
    flags=re.IGNORECASE,
)
RE_VAL_HASH = re.compile(
    r"(-\s*Hash:\s*`)([0-9a-fA-F]{32})(`)",
    flags=re.IGNORECASE,
)
RE_VAL_SIZE = re.compile(
    r"(-\s*File size:\s*`)(\d+)(`\s*bytes)",
    flags=re.IGNORECASE,
)
RE_URL = re.compile(
    r"(-\s*URL:\s*`)([^`]+)(`)",
    flags=re.IGNORECASE,
)


@dataclass
class ReleaseInfo:
    md5: str
    size_bytes: int
    url: Optional[str] = None


def find_ogg_files(directory: Path) -> List[Tuple[int, Path]]:
    """
    Finds files matching INDEX-MD5.ogg and returns a sorted list of (index, path).
    Errors if duplicate indices are found (with different hashes).
    """
    by_index: Dict[int, Path] = {}
    for p in directory.glob("*.ogg"):
        m = OGG_PATTERN.match(p.name)
        if not m:
            continue
        idx = int(m.group("index"))
        if idx in by_index:
            raise ValueError(
                f"Duplicate index {idx} with multiple hashed files: "
                f"{by_index[idx].name} and {p.name}"
            )
        by_index[idx] = p

    items = sorted(by_index.items(), key=lambda t: t[0])
    return items


def compute_md5(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def create_archive(pairs: List[Tuple[int, Path]], archive_path: Path) -> None:
    """
    Create a tar.gz where files are named as {index}.ogg (hash removed).
    """
    if archive_path.exists():
        archive_path.unlink()
    with tarfile.open(archive_path, "w:gz") as tar:
        for index, path in pairs:
            arcname = f"{index}.ogg"
            tar.add(path, arcname=arcname)


def update_readme(readme_path: Path, info: ReleaseInfo) -> None:
    """
    Update README.md with MD5, size (bytes), and optionally URL.
    If a pattern is not found, warn but continue.
    Writes the updated content back to README.md if there's any change.
    """
    if not readme_path.exists():
        print(f"README not found at {readme_path}, skipping update.")
        return

    original = readme_path.read_text(encoding="utf-8")

    # Replace MD5 in the descriptive block
    def _repl_md5(m: re.Match) -> str:
        return f"{m.group(1)}{info.md5}{m.group(3)}"

    # Replace MD5 in Valetudo Hash line
    def _repl_val_hash(m: re.Match) -> str:
        return f"{m.group(1)}{info.md5}{m.group(3)}"

    # Replace file size
    def _repl_size(m: re.Match) -> str:
        return f"{m.group(1)}{info.size_bytes}{m.group(3)}"

    # Replace URL (only if provided; otherwise leave it unchanged)
    def _repl_url(m: re.Match) -> str:
        if info.url is None:
            return m.group(0)  # no change
        return f"{m.group(1)}{info.url}{m.group(3)}"

    new_text, n_md5_block = RE_MD5_BLOCK.subn(_repl_md5, original)
    new_text, n_val_hash = RE_VAL_HASH.subn(_repl_val_hash, new_text)
    new_text, n_size = RE_VAL_SIZE.subn(_repl_size, new_text)
    new_text, n_url = RE_URL.subn(_repl_url, new_text)

    # Log what happened
    if n_md5_block == 0:
        print("Warning: MD5 block not found in README; no replacement made.")
    if n_val_hash == 0:
        print("Warning: Valetudo 'Hash:' line not found in README; no replacement made.")
    if n_size == 0:
        print("Warning: 'File size:' line not found in README; no replacement made.")
    if n_url == 0:
        print("Note: URL line not found or unchanged in README (this is okay).")

    # Write back only if anything changed
    if new_text != original:
        readme_path.write_text(new_text, encoding="utf-8")
        print(
            f"README updated: MD5-block={n_md5_block}, Hash={n_val_hash}, Size={n_size}, URL={n_url}"
        )
    else:
        print("README unchanged (no matching patterns or values identical).")


def main():
    print("🔍 Scanning for OGG files...")

    pairs = find_ogg_files(OUT_DIR)
    if not pairs:
        print(f"No OGG files found in {OUT_DIR}")
        return

    print(f"📦 Creating archive: {ARCHIVE_PATH}")
    create_archive(pairs, ARCHIVE_PATH)

    md5 = compute_md5(ARCHIVE_PATH)
    size = ARCHIVE_PATH.stat().st_size

    # Uses your existing GitHub RAW URL
    release_url = "https://github.com/padok/voice_pack_dreame/raw/main/voice_pack.tar.gz"

    info = ReleaseInfo(
        md5=md5,
        size_bytes=size,
        url=release_url
    )

    print("📝 Updating README.md")
    update_readme(README_PATH, info)

    print("✨ README.md updated successfully!")
    print(f"MD5:        {md5}")
    print(f"File size:  {size} bytes")
    print(f"URL:        {release_url}")


if __name__ == "__main__":
    main()
