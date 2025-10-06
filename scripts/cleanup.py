#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/cleanup.py
# Purpose: FastAPI usaccidents_app to ingest, normalize, and serve incidents.
#
# Author: Tim Canady
# Created: 2025-10-04
#
# Version: 0.4.1
# Last Modified: 2025-10-04 by Tim Canady
#
# Revision History:
# - 0.4.1 (2025-10-04): Auto-synced update — Tim Canady
# - 0.4.0 (2025-10-04): Initial version — Tim Canady
###################################################################
#
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
###################################################################
# Project: USAccidents
# File: scripts/cleanup.py
#
# Author: Tim Canady
# Date: Sep 2025
#
# Description:
#   Repo housekeeping utilities placeholder.
#   Example: prune temp files, verify .githooks path, etc.
#
# Version: 0.4.1
#
# Revision History
# - 0.1.1 (2025-09-30): Add skeleton and prints — Tim Canady
# - 0.1.0 (2025-09-28): Initial — Tim Canady
###################################################################
#

import os
import shutil

def main():
    print("Nothing to clean right now. Add tasks as needed.")
    # Example:
    # for p in ["./.pytest_cache", "./.mypy_cache"]:
    #     if os.path.exists(p):
    #         shutil.rmtree(p)
    #         print(f"Removed {p}")

if __name__ == "__main__":
    main()


from usaccidents_app.database import SessionLocal
from usaccidents_app import models

def remove_mock_events():
    db = SessionLocal()
    try:
        deleted = (
            db.query(models.Incident)
            .filter(models.Incident.source_event_id.like("MOCK-EVT%"))
            .delete(synchronize_session=False)
        )
        db.commit()
        print(f"Deleted {deleted} mock events.")
    finally:
        db.close()

if __name__ == "__main__":
    remove_mock_events()