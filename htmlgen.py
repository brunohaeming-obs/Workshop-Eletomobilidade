"""HTML and deploy generation entrypoints.

The implementations still live in `analysis.py`; this wrapper keeps the new
modular CLI working without duplicating the large HTML templates.
"""

from analysis import (
    build_componentes_payload,
    build_ncm_payload,
    build_payload,
    create_deploy_package,
    externalize_payload,
    write_componentes_html,
    write_html,
    write_ncm_html,
)

