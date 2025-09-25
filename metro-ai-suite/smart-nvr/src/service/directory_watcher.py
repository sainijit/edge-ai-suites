# Copyright (C) 2025 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

import time
import os
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Timer, Thread, Lock
from utils.common import settings, logger
from utils.utils import upload_videos_to_dataprep
from service.redis_store import (
    save_camera_watcher_mapping,
    load_camera_watcher_mapping,
)
from typing import Callable, Set, Dict, Optional

# -----------------------------------------------------------------------------
# Global state for camera-based directory watching
# -----------------------------------------------------------------------------

_observer: Optional[Observer] = None
_handler: Optional["DebouncedHandler"] = None
_enabled_cameras: Dict[str, bool] = {}
_mapping_lock = Lock()
_current_debounce: Optional[int] = None

# Root paths to watch. Support legacy single path plus optional list (comma separated)
_primary_root = getattr(settings, "WATCH_DIRECTORY_CONTAINER_PATH", None)
_extra_paths_raw = getattr(settings, "WATCH_DIRECTORY_CONTAINER_PATHS", "")
_root_watch_paths = []
if _primary_root:
    _root_watch_paths.append(_primary_root)
if _extra_paths_raw:
    for p in [x.strip() for x in _extra_paths_raw.split(",") if x.strip()]:
        if p not in _root_watch_paths:
            _root_watch_paths.append(p)
if not _root_watch_paths:
    logger.warning("No watch root paths configured (WATCH_DIRECTORY_CONTAINER_PATH or WATCH_DIRECTORY_CONTAINER_PATHS)")

initial_upload_status = {"total": 0, "completed": 0, "pending": 0}


class DebouncedHandler(FileSystemEventHandler):
    last_updated = None  # Class-level attribute
    lock = Lock()  # Lock for thread safety

    def __init__(self, debounce_time: int, action: Callable[[Set[str]], None]):
        self.debounce_time = debounce_time
        self.action = action
        self.timer: Optional[Timer] = None
        self.file_paths: Set[str] = set()
        self.first_event_time: Optional[datetime] = None

    def _camera_enabled(self, file_path: str) -> bool:
        """Determine the camera name from the file path relative to root and check if enabled."""
        try:
            root_used = None
            rel = None
            for root in _root_watch_paths:
                try:
                    rel_candidate = os.path.relpath(file_path, root)
                    # If file is not under this root, relpath will traverse up (start with '..')
                    if rel_candidate.startswith('..'):
                        continue
                    rel = rel_candidate
                    root_used = root
                    break
                except Exception:
                    continue
            if rel is None:
                logger.debug(f"File {file_path} not under any configured root paths {_root_watch_paths}")
                return False
            parts = rel.split(os.sep)
            camera = None
            # Frigate recordings default layout: <date>/<hour>/<camera>/<segment>.mp4
            # Where <date>=YYYY-MM-DD, <hour>=HH (00-23). We detect this pattern.
            if len(parts) >= 4:
                date_part, hour_part = parts[0], parts[1]
                if _looks_like_date(date_part) and hour_part.isdigit() and len(hour_part) == 2:
                    camera = parts[2]
            if camera is None:
                # Fallback: assume first part is the camera (legacy layout)
                camera = parts[0]
            with _mapping_lock:
                enabled = _enabled_cameras.get(camera, False)
            if not enabled:
                logger.debug(f"Ignoring file {file_path} (camera '{camera}' not enabled | root={root_used})")
            return enabled
        except Exception as e:
            logger.debug(f"Could not determine camera for {file_path}: {e}")
            return False

    def on_created(self, event):
        if event.is_directory:
            return
        if not event.src_path.endswith(".mp4"):
            return
        if not os.path.exists(event.src_path):  # transient
            return
        try:
            if os.path.getsize(event.src_path) <= 524288:  # ~512KB threshold
                return
        except OSError:
            return

        if not self._camera_enabled(event.src_path):
            return

        logger.info(f"[Watcher] on_created accepted file: {event.src_path}")
        with self.lock:
            self.file_paths.add(event.src_path)
        self._debounce()

    def on_modified(self, event):
        if event.is_directory:
            return
        if not event.src_path.endswith(".mp4"):
            return
        if not os.path.exists(event.src_path):
            return
        try:
            if os.path.getsize(event.src_path) <= 524288:
                return
        except OSError:
            return

        if not self._camera_enabled(event.src_path):
            return

        logger.debug(f"[Watcher] on_modified accepted file: {event.src_path}")
        with self.lock:
            self.file_paths.add(event.src_path)
        self._debounce()

    def _debounce(self):
        if self.first_event_time is None:
            self.first_event_time = datetime.now()
            self.timer = Timer(self.debounce_time, self._process_files)
            logger.debug(f"[Watcher] Debounce timer started (debounce={self.debounce_time}s) with {len(self.file_paths)} pending files")
            self.timer.start()
        else:
            elapsed_time = (datetime.now() - self.first_event_time).total_seconds()
            if elapsed_time >= self.debounce_time:
                logger.debug(f"[Watcher] Debounce interval elapsed; processing {len(self.file_paths)} files now")
                self._process_files()

    def _process_files(self):
        def run_action():
            global initial_upload_status
            try:
                with self.lock:
                    if not self.file_paths:
                        logger.debug("[Watcher] _process_files invoked but no files queued")
                    else:
                        logger.info(f"[Watcher] Processing batch of {len(self.file_paths)} files:")
                        for p in list(self.file_paths):
                            logger.info(f"  - {p}")
                    initial_upload_status["total"] += len(self.file_paths)
                    initial_upload_status["pending"] += len(self.file_paths)
                    start_ts = time.time()
                    try:
                        result = self.action(self.file_paths)
                    except Exception as action_err:
                        result = False
                        logger.exception(f"[Watcher] Action raised exception on batch: {action_err}")
                    duration = time.time() - start_ts
                    if result:
                        logger.info(f"[Watcher] Batch action success (files={len(self.file_paths)} elapsed={duration:.2f}s)")
                    else:
                        logger.warning(f"[Watcher] Batch action reported failure or partial success (files={len(self.file_paths)} elapsed={duration:.2f}s). Check preceding logs for details.")
                    initial_upload_status["completed"] += len(self.file_paths)
                    initial_upload_status["pending"] -= len(self.file_paths)
                    self.file_paths.clear()
                    self.first_event_time = None
            except Exception as e:
                logger.error(f"Error in _process_files: {str(e)}")
            finally:
                DebouncedHandler.last_updated = datetime.now()
                logger.info(f"Last updated time set to {DebouncedHandler.last_updated}")

        action_thread = Thread(target=run_action)
        action_thread.start()


def upload_initial_videos(path):
    global initial_upload_status
    logger.debug(f"Starting initial upload of videos from {path}")
    
    video_files = []
    if settings.WATCH_DIRECTORY_RECURSIVE:
        # Recursively find all MP4 files in subdirectories
        for root, dirs, files in os.walk(path):
            for f in files:
                if f.endswith(".mp4"):
                    file_path = os.path.join(root, f)
                    if os.path.getsize(file_path) > 524288:
                        video_files.append(file_path)
        logger.debug(f"Recursive scan found {len(video_files)} video files for initial upload")
    else:
        # Only scan the top-level directory
        video_files = [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.endswith(".mp4") and os.path.getsize(os.path.join(path, f)) > 524288
        ]
        logger.debug(f"Top-level scan found {len(video_files)} video files for initial upload")
    
    initial_upload_status["total"] = len(video_files)
    initial_upload_status["pending"] = len(video_files)

    def upload_batch(batch):
        global initial_upload_status
        try:
            logger.debug(f"Uploading batch of {len(batch)} videos: {batch}")
            success = upload_videos_to_dataprep(batch)
            if success:
                initial_upload_status["completed"] += len(batch)
                initial_upload_status["pending"] -= len(batch)
                logger.debug(
                    f"Batch upload complete. Completed: {initial_upload_status['completed']}, Pending: {initial_upload_status['pending']}"
                )
                if settings.DELETE_PROCESSED_FILES:
                    for file_path in batch:
                        os.remove(file_path)
                        logger.info(f"Deleted processed file {file_path}")
            else:
                logger.error(f"Batch upload failed for batch: {batch}")
        except Exception as e:
            logger.error(f"Error in upload_batch: {str(e)}")

    for i in range(0, len(video_files), 10):
        batch = video_files[i : i + 10]
        batch_thread = Thread(target=upload_batch, args=(batch,))
        batch_thread.start()
        logger.debug(f"Started thread for batch {i//10 + 1}")


def _ensure_watcher_running(action: Callable[[Set[str]], None], debounce_time: int):
    """Start watchers for all configured root paths if not already active."""
    global _observer, _handler, _current_debounce
    if not _root_watch_paths:
        logger.error("No watch root paths configured. Cannot start watcher.")
        return
    # Ensure all root paths exist
    for rp in _root_watch_paths:
        if not os.path.exists(rp):
            try:
                os.makedirs(rp, exist_ok=True)
            except Exception as e:
                logger.error(f"Failed to create watch root {rp}: {e}")
    if _observer is None:
        _handler = DebouncedHandler(debounce_time, action)
        _observer = Observer()
        for rp in _root_watch_paths:
            recursive = True
            _observer.schedule(_handler, rp, recursive=recursive)
            logger.info(f"Scheduled watcher on {rp} (recursive) debounce={debounce_time}s")
        _observer.start()
        _current_debounce = debounce_time
        # Log initial listings
        for rp in _root_watch_paths:
            try:
                listing = os.listdir(rp)
                logger.debug(f"[Watcher] Initial directory listing under {rp}: {listing}")
            except Exception as e:
                logger.debug(f"[Watcher] Could not list root path {rp}: {e}")
    else:
        if debounce_time != _current_debounce and _handler:
            logger.info(f"Updating debounce time from {_current_debounce} to {debounce_time}")
            _handler.debounce_time = debounce_time
            _current_debounce = debounce_time


async def set_camera_watcher_mapping(
    mapping: Dict[str, bool],
    debounce_time: int,
    action: Callable[[Set[str]], None],
    request=None,
):
    """Enable/disable cameras for the directory watcher, persist to Redis, and start watcher if needed.

    Args:
        mapping: Dict of camera -> enabled bool
        debounce_time: debounce seconds for batch processing
        action: function to call with set of file paths
        request: FastAPI request (for Redis client)
    """
    global _enabled_cameras
    with _mapping_lock:
        _enabled_cameras.update(mapping)
        # Remove cameras explicitly set to False but keep for persistence
        effective = {k: v for k, v in _enabled_cameras.items() if v}
    newly_enabled = [k for k, v in mapping.items() if v]
    newly_disabled = [k for k, v in mapping.items() if not v]
    logger.info(
        f"[Watcher] Mapping updated. Newly enabled: {newly_enabled or 'None'} | Newly disabled: {newly_disabled or 'None'} | Full mapping: {_enabled_cameras}"
    )

    # Persist full mapping (including disabled) so UI can restore state
    try:
        await save_camera_watcher_mapping(_enabled_cameras, request)
        logger.debug("[Watcher] Persisted camera watcher mapping to Redis")
    except Exception as e:
        logger.error(f"Failed to persist camera watcher mapping to Redis: {e}")

    # Ensure watcher running (even if no camera enabled yet, so future enables see events immediately)
    _ensure_watcher_running(action, debounce_time)

    # Optionally perform an initial scan for newly enabled cameras
    if newly_enabled:
        logger.info(f"[Watcher] Performing initial scan for newly enabled cameras: {newly_enabled}")
        Thread(target=_initial_scan_for_cameras, args=(newly_enabled, action), daemon=True).start()

    return _enabled_cameras


def _initial_scan_for_cameras(cameras: list[str], action: Callable[[Set[str]], None]):
    """Scan existing directory tree for mp4 files belonging to newly enabled cameras and process them."""
    try:
        if not _root_watch_paths:
            return
        batch: Set[str] = set()
        # Iterate over all roots and walk expected Frigate layout.
        for root_base in _root_watch_paths:
            try:
                date_dirs = [d for d in os.listdir(root_base) if _looks_like_date(d)]
            except Exception as e:
                logger.debug(f"[Watcher] Could not list root {root_base} for initial scan: {e}")
                continue
            for date_dir in date_dirs:
                date_path = os.path.join(root_base, date_dir)
                if not os.path.isdir(date_path):
                    continue
                try:
                    hour_dirs = [h for h in os.listdir(date_path) if len(h) == 2 and h.isdigit()]
                except Exception:
                    continue
                for hour_dir in hour_dirs:
                    hour_path = os.path.join(date_path, hour_dir)
                    if not os.path.isdir(hour_path):
                        continue
                    for camera in cameras:
                        cam_path = os.path.join(hour_path, camera)
                        if not os.path.isdir(cam_path):
                            continue
                        for root, _, files in os.walk(cam_path):
                            for f in files:
                                if not f.endswith('.mp4'):
                                    continue
                                fp = os.path.join(root, f)
                                try:
                                    if os.path.getsize(fp) > 524288:
                                        batch.add(fp)
                                except OSError:
                                    continue
        if batch:
            logger.info(f"[Watcher] Initial scan found {len(batch)} existing files for newly enabled cameras {cameras}. Dispatching action.")
            action(batch)
        else:
            logger.info(f"[Watcher] Initial scan found no existing files for cameras {cameras} above size threshold.")
    except Exception as e:
        logger.error(f"Error during initial camera scan: {e}")


def _looks_like_date(s: str) -> bool:
    """Return True if string matches YYYY-MM-DD."""
    if len(s) != 10:
        return False
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False


async def restore_camera_watchers_from_redis(action: Callable[[Set[str]], None], debounce_time: int, request=None):
    """Restore mapping from Redis at startup (call from FastAPI startup)."""
    try:
        logger.info("[Watcher] Attempting to restore camera watcher mapping from Redis...")
        stored = await load_camera_watcher_mapping(request)
        if stored:
            await set_camera_watcher_mapping(stored, debounce_time, action, request)
            logger.info(f"[Watcher] Restored camera watcher mapping from Redis: {stored}")
        else:
            logger.info("[Watcher] No stored camera watcher mapping found in Redis (starting with empty state).")
    except Exception as e:
        logger.error(f"[Watcher] Failed to restore camera watcher mapping: {e}")


def get_initial_upload_status():
    return initial_upload_status


def get_last_updated():
    return DebouncedHandler.last_updated


def get_enabled_cameras() -> Dict[str, bool]:
    with _mapping_lock:
        return dict(_enabled_cameras)