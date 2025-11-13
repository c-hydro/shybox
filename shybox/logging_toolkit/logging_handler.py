"""
Class Features

Name:          logging_handler
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251107'
Version:       '1.3.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import sys
import threading
from contextlib import contextmanager
from typing import Optional, Tuple, Dict

from contextvars import ContextVar
_CURRENT_LOGGER: ContextVar["LoggingManager | None"] = ContextVar("_CURRENT_LOGGER", default=None)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# defaults
log_file     = 'shybox.log'
log_folder   = None
log_handler  = ['file', 'stream']
log_format   = (
    '%(asctime)s %(name)-12s %(levelname)-8s '
    '%(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()] '
)
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# LoggingPrinter — depth/bookkeeping + rendering of the arrow prefix (internal)
# --------------------------------------------------------------------------------------
class LoggingPrinter:
    """Manages arrow formatting and per-tag depth tracking (thread-safe singleton)."""
    _instance = None
    _lock = threading.RLock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, arrow_suffix: str = '>', arrow_prefix: str = '-') -> None:
        # Idempotent init for singleton
        if getattr(self, "_initialized", False):
            return
        self.arrow_suffix = arrow_suffix
        self.arrow_prefix = arrow_prefix
        self.tag_not_defined = ''
        self.depth_by_tag: Dict[str, int] = {}
        self._initialized = True

    # ---- Depth management ----
    def get_depth(self, tag: Optional[str] = None) -> int:
        with self._lock:
            tag = tag or self.tag_not_defined
            return max(0, int(self.depth_by_tag.get(tag, 0)))

    def set_depth(self, value: int, tag: Optional[str] = None) -> int:
        with self._lock:
            tag = tag or self.tag_not_defined
            self.depth_by_tag[tag] = max(0, int(value))
            return self.depth_by_tag[tag]

    def push_depth(self, step: int = 1, tag: Optional[str] = None) -> int:
        return self.set_depth(self.get_depth(tag) + step, tag)

    def pop_depth(self, step: int = 1, tag: Optional[str] = None) -> int:
        return self.set_depth(self.get_depth(tag) - step, tag)

    def reset_depth(self, tag: Optional[str] = None):
        with self._lock:
            if tag:
                self.depth_by_tag.pop(tag, None)
            else:
                self.depth_by_tag.clear()

    # ---- Rendering ----
    def render(self, base_len: int, visual_depth: int) -> str:
        d = max(0, base_len + max(0, visual_depth))
        return (self.arrow_prefix * d) + self.arrow_suffix + " "


# --------------------------------------------------------------------------------------
# LoggingManager — the main convenience wrapper
# --------------------------------------------------------------------------------------
class LoggingManager:
    """Unified logger with depth arrows and persistent visual control.

    Features:
      - Thread-safe root setup
      - Depth management (begin/end push/pop) per tag
      - Persistent visual control via mode_up/mode_down (per tag), clamped at base
      - Cross-method/class stored widths keyed by (tag, store)
      - info_up/info_down wrappers (default store = tag), info_header
      - Auto tag generation (default_0, default_1, ...) if no tag is provided
      - last_prefix_len, set_prefix_len, reset_prefix_len, reset
      - Object comparison (__lt__/__gt__/…) and merge_with(max|min) utilities

    Depth semantics:
      * `begin=True` increases the logical depth **before** rendering the line,
        so the first line inside a span prints at the deeper indentation.
      * `end=True` prints at the current depth, then pops one level **after** printing.
      * `warning()` and `error()` mirror `info()` for begin/end behavior.

    ArrowPrinter compatibility helpers:
      * Use `info_header()` for header/blank lines.
      * Use `LoggingManager.rule_line()` to generate break lines (replaces `arrow_main_break`).
      * Use `warning_fixed_prefix`/`error_fixed_prefix` in `setup()` to emulate fixed prefixes like `===>`.
    """

    _root_configured = False
    _root_lock = threading.RLock()
    _log_path: Optional[str] = None

    # Global arrow defaults (configurable via setup())
    _global_arrow_base_len = 3
    _global_arrow_prefix    = "-"
    _global_arrow_suffix    = ">"

    # per-severity body chars
    _global_warning_prefix = "="
    _global_error_prefix = "!"

    # per-severity rendering behavior
    _global_warning_dynamic: bool = True
    _global_error_dynamic: bool = True
    _global_warning_fixed_prefix: Optional[str] = None  # e.g., "===> "
    _global_error_fixed_prefix: Optional[str] = None    # e.g., "!!!> "

    # ---------- Root setup ----------
    @classmethod
    def setup(
        cls,
        logger_folder: Optional[str] = None,
        logger_file: Optional[str] = None,
        logger_format: Optional[str] = None,
        level: int = logging.DEBUG,
        handlers: Optional[list] = None,
        *,
        arrow_base_len: int = 3,
        arrow_prefix: str = "-",
        arrow_suffix: str = ">",
        warning_prefix: str = "=",
        error_prefix: str = "!",
        force_reconfigure: bool = False,
        warning_dynamic: bool = True,
        error_dynamic: bool = True,
        warning_fixed_prefix: Optional[str] = None,
        error_fixed_prefix: Optional[str] = None,
    ):
        """Configure the root logger once (thread-safe) and set global arrow defaults.

        Reconfigure (Point 3):
          - If `force_reconfigure=True`, existing root handlers are removed and
            rebuilt using the provided parameters (level/format/handlers).
          - If the root is already configured and `force_reconfigure=False`, we
            only update global defaults/flags and return.

        Severity behavior:
          - `warning_dynamic` / `error_dynamic`: if True, warning/error follow the
            depth-based arrow like info. If False, they use the base length only.
          - `warning_fixed_prefix` / `error_fixed_prefix`: if provided, they are
            used literally (e.g., "===> ") and override dynamic behavior.
        """
        with cls._root_lock:
            if cls._root_configured and not force_reconfigure:
                cls._global_arrow_base_len = arrow_base_len
                cls._global_arrow_prefix = arrow_prefix
                cls._global_arrow_suffix = arrow_suffix
                cls._global_warning_prefix = warning_prefix
                cls._global_error_prefix = error_prefix
                cls._global_warning_dynamic = bool(warning_dynamic)
                cls._global_error_dynamic = bool(error_dynamic)
                cls._global_warning_fixed_prefix = warning_fixed_prefix
                cls._global_error_fixed_prefix = error_fixed_prefix
                return

            folder_default = globals().get("log_folder") or os.getcwd()
            folder = logger_folder if logger_folder not in (None, "") else folder_default
            file_name = logger_file or (globals().get("log_file") or "shybox.log")
            fmt = logger_format or (globals().get("log_format") or "%(message)s")
            effective_handlers = handlers if handlers is not None else (globals().get("log_handler") or ["file", "stream"])

            os.makedirs(folder, exist_ok=True)
            log_path = os.path.join(folder, file_name)

            formatter = logging.Formatter(fmt)
            root = logging.getLogger()

            if force_reconfigure and root.handlers:
                for h in list(root.handlers):
                    root.removeHandler(h)
                    try:
                        h.close()
                    except Exception:
                        pass

            root.setLevel(level)

            if "file" in effective_handlers:
                fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
                fh.setFormatter(formatter)
                root.addHandler(fh)

            if "stream" in effective_handlers:
                ch = logging.StreamHandler(sys.stdout)
                ch.setFormatter(formatter)
                root.addHandler(ch)

            # Store global arrow defaults and behavior
            cls._global_arrow_base_len = arrow_base_len
            cls._global_arrow_prefix = arrow_prefix
            cls._global_arrow_suffix = arrow_suffix
            cls._global_warning_prefix = warning_prefix
            cls._global_error_prefix = error_prefix
            cls._global_warning_dynamic = bool(warning_dynamic)
            cls._global_error_dynamic = bool(error_dynamic)
            cls._global_warning_fixed_prefix = warning_fixed_prefix
            cls._global_error_fixed_prefix = error_fixed_prefix

            cls._root_configured = True
            cls._log_path = log_path

    # ---------- Instance ----------
    def __init__(
        self,
        name: Optional[str] = None,
        level: int = logging.INFO,
        use_arrows: bool = True,
        arrow_dynamic: bool = True,
        arrow_tag: Optional[str] = None,
        arrow_base_len: Optional[int] = None,  # per-instance override
        *,
        propagate: Optional[bool] = None,      # control logger propagation
        set_as_current: bool = False
    ):
        if not self._root_configured:
            self.setup()

        self.logger = logging.getLogger(name or "default")
        self.logger.setLevel(level)

        if propagate is not None:
            self.logger.propagate = bool(propagate)

        self.use_arrows = use_arrows
        self.arrow_dynamic = arrow_dynamic
        self.arrow_tag = arrow_tag
        self.arrow_base_len = arrow_base_len if arrow_base_len is not None else self._global_arrow_base_len

        self._printer = LoggingPrinter() if use_arrows else None
        if self._printer:
            self._printer.arrow_prefix = self._global_arrow_prefix
            self._printer.arrow_suffix = self._global_arrow_suffix

        self._state_lock = threading.RLock()
        self._abs_prefix_len: Dict[str, int] = {}
        self._last_prefix_len: Dict[str, int] = {}
        self._stores: Dict[Tuple[str, str], int] = {}
        self._tag_counter = 0
        self._last_tag_used = None

        # Register as current if requested
        if set_as_current:
            LoggingManager.set_current(self)

    # ---------- Tag helpers ----------
    def _remember_tag(self, tag: Optional[str]) -> str:
        with self._state_lock:
            if tag is not None:
                self._last_tag_used = tag
                return tag
            if self._last_tag_used is not None:
                return self._last_tag_used
            t = f"default_{self._tag_counter}"
            self._tag_counter += 1
            self._last_tag_used = t
            return t

    def _tagkey(self, tag: Optional[str]) -> str:
        return self._remember_tag(tag)

    # ---------- Current-logger context (no explicit param needed) ----------
    @classmethod
    def set_current(cls, log: "LoggingManager") -> None:
        """Set the current logger for this context (thread/async-safe)."""
        _CURRENT_LOGGER.set(log)

    @classmethod
    def get_current(cls) -> "LoggingManager | None":
        """Return current logger if set, else None."""
        return _CURRENT_LOGGER.get()

    @classmethod
    def require_current(cls) -> "LoggingManager":
        """Return current logger or raise a clear error if none was set."""
        log = _CURRENT_LOGGER.get()
        if log is None:
            raise RuntimeError(
                "No current LoggingManager set. Call LoggingManager.set_current(...) at startup."
            )
        return log

    def use_as_current(self):
        """Context manager to install this logger as current within a block."""
        from contextlib import contextmanager
        from contextvars import Token

        @contextmanager
        def _cm():
            token: Token = _CURRENT_LOGGER.set(self)
            try:
                yield self
            finally:
                _CURRENT_LOGGER.reset(token)
        return _cm()

    # ---------- Stores ----------
    def _store_key(self, tag: Optional[str], store: str) -> Tuple[str, str]:
        return (self._tagkey(tag), str(store))

    def store_set(self, store: str, n: int, tag: Optional[str] = None) -> None:
        with self._state_lock:
            self._stores[self._store_key(tag, store)] = max(self.arrow_base_len, int(n))

    def store_get(self, store: str, tag: Optional[str] = None, default: Optional[int] = None) -> Optional[int]:
        with self._state_lock:
            return self._stores.get(self._store_key(tag, store), default)

    def store_clear(self, store: str, tag: Optional[str] = None) -> None:
        with self._state_lock:
            self._stores.pop(self._store_key(tag, store), None)

    def apply_store(self, store: str, tag: Optional[str] = None) -> None:
        t = self._tagkey(tag)
        n = self.store_get(store, tag=t)
        if n is not None:
            self.set_prefix_len(n, tag=t)

    # ---------- Persistent visual control ----------
    def last_prefix_len(self, tag: Optional[str] = None) -> int:
        t = self._tagkey(tag)
        with self._state_lock:
            return int(self._last_prefix_len.get(t, self.arrow_base_len))

    def set_prefix_len(self, n: int, tag: Optional[str] = None, *, update_last: bool = True) -> None:
        t = self._tagkey(tag)
        v = max(self.arrow_base_len, int(n))
        with self._state_lock:
            self._abs_prefix_len[t] = v
            if update_last:
                self._last_prefix_len[t] = v

    def _render_prefix(self, *, mode: int, tag: Optional[str],
                       body_char: Optional[str] = None,
                       tip_char: Optional[str] = None) -> str:
        n = self._compute_prefix_len(mode=mode, tag=tag)
        t = self._tagkey(tag)
        with self._state_lock:
            self._last_prefix_len[t] = n

        if not self.use_arrows or self._printer is None:
            return ""

        body = (body_char if body_char is not None else self._global_arrow_prefix)
        tip = (tip_char if tip_char is not None else self._global_arrow_suffix)
        return (body * n) + tip + " "

    def _render_fixed(self, fixed_prefix: str) -> str:
        # Render a literal prefix (caller ensures trailing space if desired)
        return fixed_prefix if fixed_prefix.endswith(" ") else (fixed_prefix + " ")

    def _render_static_base(self, *, tag: Optional[str], body_char: Optional[str], tip_char: Optional[str]) -> str:
        # Render using base length only (no depth influence)
        n = max(0, self.arrow_base_len)
        t = self._tagkey(tag)
        with self._state_lock:
            self._last_prefix_len[t] = n
        body = (body_char if body_char is not None else self._global_arrow_prefix)
        tip = (tip_char if tip_char is not None else self._global_arrow_suffix)
        return (body * n) + tip + " "

    def reset_prefix_len(self, tag: Optional[str] = None) -> None:
        t = self._tagkey(tag)
        with self._state_lock:
            self._abs_prefix_len.pop(t, None)

    def reset(self, tag: Optional[str] = None) -> None:
        if self._printer:
            self._printer.reset_depth(tag=tag)
        with self._state_lock:
            if tag is None:
                self._abs_prefix_len.clear()
                self._last_prefix_len.clear()
                self._stores.clear()
            else:
                t = self._tagkey(tag)
                self._abs_prefix_len.pop(t, None)
                self._last_prefix_len.pop(t, None)
                for k in [k for k in list(self._stores.keys()) if k[0] == t]:
                    self._stores.pop(k, None)

    def mode_up(self, step: int = 1, tag: Optional[str] = None) -> int:
        t = self._tagkey(tag)
        s = max(1, int(step))
        with self._state_lock:
            cur = self._abs_prefix_len.get(t)
            if cur is None:
                cur = self.arrow_base_len
            self._abs_prefix_len[t] = max(self.arrow_base_len, cur + s)
        return 0

    def mode_down(self, step: int = 1, tag: Optional[str] = None) -> int:
        t = self._tagkey(tag)
        s = max(1, int(step))
        with self._state_lock:
            cur = self._abs_prefix_len.get(t)
            if cur is None:
                cur = self.arrow_base_len
            self._abs_prefix_len[t] = max(self.arrow_base_len, cur - s)
        return 0

    # ---------- Depth helpers ----------
    def depth(self, tag: Optional[str] = None) -> int:
        return self._printer.get_depth(self._tagkey(tag)) if self._printer else 0

    # ---------- Prefix computation/rendering ----------
    def _compute_prefix_len(self, *, mode: int, tag: Optional[str]) -> int:
        t = self._tagkey(tag)

        with self._state_lock:
            if t in self._abs_prefix_len:
                return self._abs_prefix_len[t]

        if not self.use_arrows or self._printer is None:
            return max(0, self.arrow_base_len)

        depth = self._printer.get_depth(t) if self.arrow_dynamic else 0
        visual_depth = max(0, depth - 1 + (mode or 0))
        return max(0, self.arrow_base_len + visual_depth)

    # ---------- Core logging ----------
    def info(self, msg: str, *args, mode: int = 0, tag: Optional[str] = None,
             begin: bool = False, end: bool = False,
             style: Optional[str] = None,
             **kwargs):
        tag = self._tagkey(tag)

        if style in ("header", "title", "none"):
            self.logger.info(msg, *args, **kwargs)
            return

        if end and self._printer and self.arrow_dynamic:
            prefix = self._render_prefix(mode=mode, tag=tag)
            self.logger.info(f"{prefix}{msg}", *args, **kwargs)
            self._printer.pop_depth(tag=tag)
            return

        if begin and self._printer and self.arrow_dynamic:
            self._printer.push_depth(tag=tag)

        prefix = self._render_prefix(mode=mode, tag=tag)
        self.logger.info(f"{prefix}{msg}", *args, **kwargs)

    def debug(self, msg: str, *args, mode: int = 0, tag: Optional[str] = None,
              style: Optional[str] = None, **kwargs):
        tag = self._tagkey(tag)
        if style in ("header", "title", "none"):
            self.logger.debug(msg, *args, **kwargs)
            return
        prefix = self._render_prefix(mode=mode, tag=tag)
        self.logger.debug(f"{prefix}{msg}", *args, **kwargs)

    def warning(self, msg: str, *args, mode: int = 0, tag: Optional[str] = None,
                begin: bool = False, end: bool = False,
                style: Optional[str] = None,
                body_char: Optional[str] = None,
                **kwargs):
        """WARNING with either dynamic or fixed prefix (configured in setup)."""
        tag = self._tagkey(tag)

        if style in ("header", "title", "none"):
            self.logger.warning(msg, *args, **kwargs)
            return

        if begin and self._printer and self.arrow_dynamic:
            self._printer.push_depth(tag=tag)

        if self._global_warning_fixed_prefix:
            prefix = self._render_fixed(self._global_warning_fixed_prefix)
        else:
            if self._global_warning_dynamic:
                prefix = self._render_prefix(mode=mode, tag=tag,
                                             body_char=body_char or self._global_warning_prefix)
            else:
                prefix = self._render_static_base(tag=tag,
                                                  body_char=body_char or self._global_warning_prefix,
                                                  tip_char=None)
        # End behavior handled after printing
        if end and self._printer and self.arrow_dynamic:
            self.logger.warning(f"{prefix}{msg}", *args, **kwargs)
            self._printer.pop_depth(tag=tag)
            return

        self.logger.warning(f"{prefix}{msg}", *args, **kwargs)

    def error(self, msg: str, *args, mode: int = 0, tag: Optional[str] = None,
              begin: bool = False, end: bool = False,
              style: Optional[str] = None,
              body_char: Optional[str] = None,
              **kwargs):
        """ERROR with either dynamic or fixed prefix (configured in setup)."""
        tag = self._tagkey(tag)

        if style in ("header", "title", "none"):
            self.logger.error(msg, *args, **kwargs)
            return

        if begin and self._printer and self.arrow_dynamic:
            self._printer.push_depth(tag=tag)

        if self._global_error_fixed_prefix:
            prefix = self._render_fixed(self._global_error_fixed_prefix)
        else:
            if self._global_error_dynamic:
                prefix = self._render_prefix(mode=mode, tag=tag,
                                             body_char=body_char or self._global_error_prefix)
            else:
                prefix = self._render_static_base(tag=tag,
                                                  body_char=body_char or self._global_error_prefix,
                                                  tip_char=None)
        if end and self._printer and self.arrow_dynamic:
            self.logger.error(f"{prefix}{msg}", *args, **kwargs)
            self._printer.pop_depth(tag=tag)
            return

        self.logger.error(f"{prefix}{msg}", *args, **kwargs)

    def exception(self, msg: str, *args, mode: int = 0, tag: Optional[str] = None,
                  begin: bool = False, end: bool = False,
                  style: Optional[str] = None,
                  body_char: Optional[str] = None,
                  **kwargs):
        """EXCEPTION with traceback; uses error's configured arrow rules."""
        tag = self._tagkey(tag)

        if style in ("header", "title", "none"):
            self.logger.exception(msg, *args, **kwargs)
            return

        if begin and self._printer and self.arrow_dynamic:
            self._printer.push_depth(tag=tag)

        if self._global_error_fixed_prefix:
            prefix = self._render_fixed(self._global_error_fixed_prefix)
        else:
            if self._global_error_dynamic:
                prefix = self._render_prefix(mode=mode, tag=tag,
                                             body_char=body_char or self._global_error_prefix)
            else:
                prefix = self._render_static_base(tag=tag,
                                                  body_char=body_char or self._global_error_prefix,
                                                  tip_char=None)
        if end and self._printer and self.arrow_dynamic:
            self.logger.exception(f"{prefix}{msg}", *args, **kwargs)
            self._printer.pop_depth(tag=tag)
            return

        self.logger.exception(f"{prefix}{msg}", *args, **kwargs)

    # ---------- Convenience wrappers ----------
    def info_up(self, msg: str, tag: Optional[str] = None, step: int = 1,
                store: Optional[str] = None, **kwargs):
        t = self._tagkey(tag)
        s = max(1, int(step))
        store_key = store or t
        cur = self.store_get(store_key, tag=t, default=self.arrow_base_len)
        new = max(self.arrow_base_len, cur + s)
        self.store_set(store_key, new, tag=t)
        self.set_prefix_len(new, tag=t, update_last=True)
        self.info(msg, tag=t, **kwargs)

    def info_down(self, msg: str, tag: Optional[str] = None, step: int = 1,
                  store: Optional[str] = None, align: bool = True, **kwargs):
        t = self._tagkey(tag)
        s = max(1, int(step))
        store_key = store or t
        cur = self.store_get(store_key, tag=t, default=self.arrow_base_len)
        if align:
            self.set_prefix_len(cur, tag=t, update_last=False)
        self.info(msg, tag=t, **kwargs)
        new = max(self.arrow_base_len, cur - s)
        self.store_set(store_key, new, tag=t)
        self.set_prefix_len(new, tag=t, update_last=True)

    def info_header(self, msg: str, *, blank_before: bool = False,
                    blank_after: bool = False, underline: bool = False):
        if blank_before:
            self.logger.info("")
        self.info(msg, style='header')
        if underline:
            line = "=" * len(str(msg))
            self.info(line, style='header')
        if blank_after:
            self.logger.info("")

    # ---------- Comparison utilities ----------
    def compare_prefix_len(self, tag_a: Optional[str] = None, tag_b: Optional[str] = None) -> int:
        a = self.last_prefix_len(tag_a)
        b = self.last_prefix_len(tag_b)
        return (a > b) - (a < b)

    def keep_prefix_len(self, mode: str = "max", tag: Optional[str] = None, other_tag: Optional[str] = None) -> None:
        t1 = self._tagkey(tag)
        t2 = self._tagkey(other_tag)
        len1 = self.last_prefix_len(t1)
        len2 = self.last_prefix_len(t2)
        if mode == "max":
            target = max(len1, len2)
        elif mode == "min":
            target = min(len1, len2)
        else:
            raise ValueError("mode must be 'max' or 'min'")
        self.set_prefix_len(target, tag=t1)
        self.set_prefix_len(target, tag=t2)
        self.store_set(t1, target, tag=t1)
        self.store_set(t2, target, tag=t2)

    def __lt__(self, other: "LoggingManager") -> bool:
        if not isinstance(other, LoggingManager):
            return NotImplemented
        return self.last_prefix_len() < other.last_prefix_len()

    def __le__(self, other: "LoggingManager") -> bool:
        if not isinstance(other, LoggingManager):
            return NotImplemented
        return self.last_prefix_len() <= other.last_prefix_len()

    def __gt__(self, other: "LoggingManager") -> bool:
        if not isinstance(other, LoggingManager):
            return NotImplemented
        return self.last_prefix_len() > other.last_prefix_len()

    def __ge__(self, other: "LoggingManager") -> bool:
        if not isinstance(other, LoggingManager):
            return NotImplemented
        return self.last_prefix_len() >= other.last_prefix_len()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LoggingManager):
            return NotImplemented
        return self.last_prefix_len() == other.last_prefix_len()

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, LoggingManager):
            return NotImplemented
        return self.last_prefix_len() != other.last_prefix_len()

    def compare(self, other: "LoggingManager", mode: str = "max", sync: bool = True) -> "LoggingManager":
        if not isinstance(other, LoggingManager):
            raise TypeError("Can only compare with another LoggingManager")
        len_self = self.last_prefix_len()
        len_other = other.last_prefix_len()
        if mode not in ("max", "min"):
            raise ValueError("mode must be 'max' or 'min'")
        if mode == "max":
            chosen = self if len_self >= len_other else other
            new_len = max(len_self, len_other)
        else:
            chosen = self if len_self <= len_other else other
            new_len = min(len_self, len_other)
        if sync:
            self.set_prefix_len(new_len)
            other.set_prefix_len(new_len)
        return chosen

    @property
    def log_path(self) -> Optional[str]:
        return self._log_path

    # ---------- ArrowPrinter replacement utilities ----------
    @staticmethod
    def rule_line(char: str = "=", width: int = 78) -> str:
        """Return a repeated char line, e.g., main break line."""
        if not char:
            char = "="
        if width <= 0:
            width = 1
        return char * width

    # ---------- Context manager ----------
    @contextmanager
    def span(self, msg: Optional[str] = None, tag: Optional[str] = None, level: str = "info"):
        """Increase depth for the block; optional opening message at given level.
        Depth is restored at block exit even on exception.
        """
        t = self._tagkey(tag)
        if msg:
            getattr(self, level)(msg, begin=True, tag=t)
        else:
            if self._printer and self.arrow_dynamic:
                self._printer.push_depth(tag=t)
        try:
            yield
        finally:
            if self._printer and self.arrow_dynamic:
                self._printer.pop_depth(tag=t)
