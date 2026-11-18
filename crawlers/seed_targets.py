"""Initialize a writable target list from the copy bundled with the crawler."""
import argparse
import hashlib
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from crawlers.common.targets import load_target_apps
from crawlers.config import TARGET_APP_LIST_PATH


def _write_if_missing(path: Path, data: bytes):
    """Publish a complete file without overwriting one written by another Job."""
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = NamedTemporaryFile(dir=path.parent, delete=False)
    temp = Path(stream.name)
    try:
        with stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temp, path)
    except FileExistsError:
        return False
    finally:
        temp.unlink()
    return True


def seed(source: Path, target: Path):
    target_manifest = Path(f"{target}.manifest.json")
    if target.exists() and target_manifest.exists():
        load_target_apps(target)
        return False
    if target_manifest.exists():
        raise ValueError(f"Target file missing for existing manifest {target_manifest}")

    source_manifest = Path(f"{source}.manifest.json")
    manifest = json.loads(source_manifest.read_text(encoding="utf-8"))
    load_target_apps(source)
    data = source.read_bytes()
    _write_if_missing(target, data)
    existing = target.read_bytes()
    if json.loads(existing) != json.loads(data):
        if target_manifest.exists():
            load_target_apps(target)
            return False
        raise ValueError(f"Refusing to replace existing targets at {target}")
    manifest["sha256"] = hashlib.sha256(existing).hexdigest()
    return _write_if_missing(target_manifest, json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("/app/data/target_apps.json"))
    parser.add_argument("--target", type=Path, default=Path(TARGET_APP_LIST_PATH))
    args = parser.parse_args()
    changed = seed(args.source, args.target)
    print(f"{'Initialized' if changed else 'Kept'} target list at {args.target}")


if __name__ == "__main__":
    main()
