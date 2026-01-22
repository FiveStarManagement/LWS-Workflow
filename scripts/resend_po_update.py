import os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from services.polytex_po_update import update_po_line_qty_api

resp = update_po_line_qty_api(
    po_num=237017,
    itemcode="16P4-2300-NPU01-0686T4",
    plantcode="4",
    po_linenum=1,
    new_qty=21830.0,
)

print(resp)
