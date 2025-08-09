import hashlib
import re

_SAFE_MAX = 200  # stay under typical 255-ish limits
_slug_re = re.compile(r"[^a-z0-9_]+")


def _slug(s: str) -> str:
    return _slug_re.sub("_", s.lower()).strip("_")

def build_foreign_key_name(
        catalog: str, 
        schema: str, 
        source_table: str, 
        source_columns: list[str], 
        reference_table: str
    ) -> str:
    base = f"fk_{_slug(catalog)}_{_slug(schema)}_{_slug(source_table)}__{'_'.join(_slug(c) for c in source_columns)}__ref__{_slug(reference_table)}"
    if len(base) <= _SAFE_MAX:
        return base
    # hash the full base to keep uniqueness, then truncate
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
    head = base[: (_SAFE_MAX - 1 - len(h))]
    return f"{head}_{h}"
