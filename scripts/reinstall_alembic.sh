    #!/usr/bin/env bash
    #
    ###################################################################
    # Project: USAccidents
    # File: scripts/reinstall_alembic.sh
    # Purpose: Reinstall and reinitialize Alembic safely for the project.
    #
    # Description of code and how it works:
    # - Force-reinstalls Alembic in the active interpreter.
    # - Backs up existing ./alembic and ./alembic.ini (if present).
    # - Writes fresh alembic.ini and env.py templates (env reads DATABASE_URL from .env).
    # - Restores existing migration files if found.
    # - Runs `alembic upgrade head` to validate setup.
    #
    # Author: Tim Canady
    # Created: 2025-09-28
    #
    # Version: 0.7.0
    # Last Modified: 2025-10-07 by Tim Canady
    #
    # Revision History:
    # - 0.7.0 (2025-10-07): Initial reinstall script, safe backup/restore, dotenv-aware env.py.
    ###################################################################
    #
    set -euo pipefail

    echo "[alembic] Using python: $(command -v python || true)"
    python -V || true

    # 1) Reinstall alembic + dotenv
    python -m pip install --upgrade --force-reinstall alembic==1.13.2 python-dotenv==1.0.1

    # 2) Backup existing files
    TS=$(date +%Y%m%d_%H%M%S)
    if [[ -d alembic ]]; then
      mv alembic "alembic.bak_${TS}"
      echo "[alembic] Backed up ./alembic -> alembic.bak_${TS}"
    fi
    if [[ -f alembic.ini ]]; then
      mv alembic.ini "alembic.ini.bak_${TS}"
      echo "[alembic] Backed up ./alembic.ini -> alembic.ini.bak_${TS}"
    fi

    # 3) Recreate structure
    mkdir -p alembic/versions

    # 4) Write new alembic.ini and env.py from templates in this bundle
    cp -f ./alembic.ini ./alembic.ini 2>/dev/null || true  # no-op if running from project root
    if [[ -f "/mnt/data/alembic_reinstall_bundle/alembic.ini" ]]; then
      cp "/mnt/data/alembic_reinstall_bundle/alembic.ini" ./alembic.ini
      echo "[alembic] Wrote ./alembic.ini"
    fi
    if [[ -f "/mnt/data/alembic_reinstall_bundle/alembic/env.py" ]]; then
      cp "/mnt/data/alembic_reinstall_bundle/alembic/env.py" ./alembic/env.py
      echo "[alembic] Wrote ./alembic/env.py"
    fi
    if [[ -f "/mnt/data/alembic_reinstall_bundle/alembic/script.py.mako" ]]; then
      cp "/mnt/data/alembic_reinstall_bundle/alembic/script.py.mako" ./alembic/script.py.mako
      echo "[alembic] Wrote ./alembic/script.py.mako"
    fi

    # 5) Restore existing version files if a backup exists
    LATEST_BAK=$(ls -d alembic.bak_* 2>/dev/null | sort -r | head -n1 || true)
    if [[ -n "$LATEST_BAK" && -d "$LATEST_BAK/versions" ]]; then
      echo "[alembic] Restoring previous migration files from $LATEST_BAK/versions"
      cp "$LATEST_BAK"/versions/*.py ./alembic/versions/ 2>/dev/null || true
    fi

    # 6) Verify DATABASE_URL is present
    set +e
    if ! python - <<'PY'
import os
from dotenv import load_dotenv
load_dotenv()
url=os.getenv('DATABASE_URL');print(url if url else '')
PY
 | grep -E '^.+' >/dev/null; then
      echo "[alembic] WARNING: DATABASE_URL missing. Create .env with DATABASE_URL before running 'alembic upgrade head'."
fi
    set -e

    # 7) Smoke test upgrade
    if command -v alembic >/dev/null 2>&1; then
      echo "[alembic] Running 'alembic upgrade head' (may be a no-op)"
      alembic upgrade head || { echo "[alembic] upgrade failed (this can happen if DB is unreachable)."; exit 0; }
    else
      echo "[alembic] alembic CLI not found on PATH. Ensure your venv is active."
    fi

    echo "[alembic] Reinstall complete."
