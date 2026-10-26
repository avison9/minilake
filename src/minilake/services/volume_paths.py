"""Resolve Unity Catalog volume URIs to their on-disk storage."""

from pathlib import Path

from minilake.config import settings
from minilake.errors import DatabricksError

_VOLUMES_PREFIX = "/Volumes"


def resolve_volume_path(path: str) -> Path | None:
    """Resolve a `/Volumes/...` URI to its Unity Catalog volume directory.

    Returns ``None`` for paths outside the volume namespace so Files and DBFS can
    use their ordinary shared root. Volume URIs must name an existing volume;
    this prevents deleted or unknown volumes from becoming generic DBFS paths.
    """
    normalized = "/" + path.lstrip("/")
    if normalized != _VOLUMES_PREFIX and not normalized.startswith(_VOLUMES_PREFIX + "/"):
        return None

    suffix = normalized[len(_VOLUMES_PREFIX) :]
    if suffix.startswith("/"):
        suffix = suffix[1:]
    if suffix.endswith("/"):
        suffix = suffix[:-1]
    parts = suffix.split("/")
    if len(parts) < 3 or any(part in {"", ".", ".."} for part in parts[:3]):
        raise DatabricksError(error_code="INVALID_REQUEST", message="Invalid volume path", status_code=400)

    catalog_name, schema_name, volume_name = parts[:3]
    volumes_root = (settings.data_dir / "volumes").resolve()
    volume_root = volumes_root.joinpath(catalog_name, schema_name, volume_name)

    # The catalog/schema/volume components identify the volume itself; do not
    # follow a symlink there to a different volume or outside the volume tree.
    current = volumes_root
    for part in (catalog_name, schema_name, volume_name):
        current = current / part
        if current.is_symlink():
            raise DatabricksError(error_code="INVALID_REQUEST", message="Invalid volume path", status_code=400)

    volume_root = volume_root.resolve()
    if volumes_root not in volume_root.parents or not volume_root.is_dir():
        full_name = f"{catalog_name}.{schema_name}.{volume_name}"
        raise DatabricksError(
            error_code="RESOURCE_DOES_NOT_EXIST",
            message=f"Volume '{full_name}' not found",
            status_code=404,
        )

    target = volume_root.joinpath(*parts[3:]).resolve()
    if target != volume_root and volume_root not in target.parents:
        raise DatabricksError(error_code="INVALID_REQUEST", message="Invalid path", status_code=400)
    return target
