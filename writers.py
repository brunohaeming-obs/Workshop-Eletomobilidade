"""Writers for generated JSON indices.

This module is intentionally thin for now: it exposes the writer functions from
the legacy monolithic pipeline so `main_analysis.py` can run while the codebase
continues being split into smaller modules.
"""

from analysis import write_cnpj_index, write_component_files, write_detail_files

