"""
file_cache.py — File-based caching for uploaded data files.

Caches uploaded CSVs to disk so they survive server restarts.
Files are stored in ./cache/ directory with date-based naming.

v1.5 - Initial implementation
"""

import os
import json
from datetime import date, datetime
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"


def _ensure_cache_dir():
    """Create cache directory if it doesn't exist."""
    CACHE_DIR.mkdir(exist_ok=True)


def _get_cache_path(file_type: str, plan_date: str = None) -> Path:
    """Get path for a cached file."""
    if plan_date is None:
        plan_date = date.today().strftime("%Y-%m-%d")
    return CACHE_DIR / f"{file_type}_{plan_date}.cache"


def _get_metadata_path(plan_date: str = None) -> Path:
    """Get path for cache metadata."""
    if plan_date is None:
        plan_date = date.today().strftime("%Y-%m-%d")
    return CACHE_DIR / f"metadata_{plan_date}.json"


# ─────────────────────────────────────────────────────────────────────────────
#  SAVE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def cache_file(file_type: str, content: bytes, plan_date: str = None) -> dict:
    """
    Cache a file to disk.
    
    file_type: 'scc', 'pickorder', 'dispatch_text', 'assignment_text'
    content: Raw bytes content
    plan_date: YYYY-MM-DD format (defaults to today)
    
    Returns: {success: bool, path: str, size: int}
    """
    _ensure_cache_dir()
    
    if plan_date is None:
        plan_date = date.today().strftime("%Y-%m-%d")
    
    path = _get_cache_path(file_type, plan_date)
    
    try:
        with open(path, "wb") as f:
            f.write(content if isinstance(content, bytes) else content.encode("utf-8"))
        
        # Update metadata
        _update_metadata(file_type, plan_date, len(content))
        
        return {
            "success": True,
            "path": str(path),
            "size": len(content),
            "cached_at": datetime.now().strftime("%H:%M:%S"),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def cache_text(file_type: str, text: str, plan_date: str = None) -> dict:
    """Cache text content (dispatch_text, assignment_text)."""
    return cache_file(file_type, text.encode("utf-8"), plan_date)


def _update_metadata(file_type: str, plan_date: str, size: int):
    """Update metadata JSON with file info."""
    meta_path = _get_metadata_path(plan_date)
    
    if meta_path.exists():
        with open(meta_path, "r") as f:
            meta = json.load(f)
    else:
        meta = {"plan_date": plan_date, "files": {}}
    
    meta["files"][file_type] = {
        "cached_at": datetime.now().strftime("%H:%M:%S"),
        "size": size,
    }
    meta["last_updated"] = datetime.now().strftime("%H:%M:%S")
    
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
#  LOAD FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def load_cached_file(file_type: str, plan_date: str = None) -> bytes:
    """
    Load a cached file from disk.
    
    Returns: bytes content or None if not found
    """
    if plan_date is None:
        plan_date = date.today().strftime("%Y-%m-%d")
    
    path = _get_cache_path(file_type, plan_date)
    
    if not path.exists():
        return None
    
    try:
        with open(path, "rb") as f:
            return f.read()
    except Exception:
        return None


def load_cached_text(file_type: str, plan_date: str = None) -> str:
    """Load cached text content."""
    content = load_cached_file(file_type, plan_date)
    if content:
        return content.decode("utf-8")
    return None


def get_cache_status(plan_date: str = None) -> dict:
    """
    Get status of all cached files for a date.
    
    Returns: {
        "plan_date": "2026-04-23",
        "files": {
            "scc": {"cached_at": "10:30:15", "size": 12345},
            "pickorder": {"cached_at": "10:31:00", "size": 8765},
            ...
        },
        "has_cache": True
    }
    """
    if plan_date is None:
        plan_date = date.today().strftime("%Y-%m-%d")
    
    meta_path = _get_metadata_path(plan_date)
    
    if not meta_path.exists():
        return {
            "plan_date": plan_date,
            "files": {},
            "has_cache": False,
        }
    
    try:
        with open(meta_path, "r") as f:
            meta = json.load(f)
        meta["has_cache"] = len(meta.get("files", {})) > 0
        return meta
    except Exception:
        return {
            "plan_date": plan_date,
            "files": {},
            "has_cache": False,
        }


def has_cached_file(file_type: str, plan_date: str = None) -> bool:
    """Check if a specific file is cached."""
    if plan_date is None:
        plan_date = date.today().strftime("%Y-%m-%d")
    return _get_cache_path(file_type, plan_date).exists()


# ─────────────────────────────────────────────────────────────────────────────
#  CLEAR FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def clear_cache(plan_date: str = None) -> dict:
    """
    Clear all cached files for a specific date.
    
    Returns: {success: bool, files_deleted: int}
    """
    if plan_date is None:
        plan_date = date.today().strftime("%Y-%m-%d")
    
    _ensure_cache_dir()
    
    deleted = 0
    errors = []
    
    # Delete all files matching the date pattern
    for file in CACHE_DIR.glob(f"*_{plan_date}.*"):
        try:
            file.unlink()
            deleted += 1
        except Exception as e:
            errors.append(str(e))
    
    return {
        "success": len(errors) == 0,
        "files_deleted": deleted,
        "plan_date": plan_date,
        "errors": errors if errors else None,
    }


def clear_all_cache() -> dict:
    """Clear ALL cached files (all dates)."""
    _ensure_cache_dir()
    
    deleted = 0
    errors = []
    
    for file in CACHE_DIR.glob("*"):
        try:
            file.unlink()
            deleted += 1
        except Exception as e:
            errors.append(str(e))
    
    return {
        "success": len(errors) == 0,
        "files_deleted": deleted,
        "errors": errors if errors else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def list_cached_dates() -> list:
    """List all dates that have cached data."""
    _ensure_cache_dir()
    
    dates = set()
    for file in CACHE_DIR.glob("metadata_*.json"):
        # Extract date from filename: metadata_2026-04-23.json
        date_str = file.stem.replace("metadata_", "")
        dates.add(date_str)
    
    return sorted(dates, reverse=True)  # Most recent first


def get_cache_size() -> dict:
    """Get total cache size."""
    _ensure_cache_dir()
    
    total_size = 0
    file_count = 0
    
    for file in CACHE_DIR.glob("*"):
        if file.is_file():
            total_size += file.stat().st_size
            file_count += 1
    
    return {
        "total_bytes": total_size,
        "total_mb": round(total_size / (1024 * 1024), 2),
        "file_count": file_count,
    }
