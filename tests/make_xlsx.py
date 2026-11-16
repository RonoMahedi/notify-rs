"""Writes a genuine .xlsx (Office Open XML) using only the standard library."""
import sys, zipfile
from xml.sax.saxutils import escape

def col(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def cell(ref, v):
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return f'<c r="{ref}"><v>{v}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(v))}</t></is></c>'

CITIES = ["Dhaka", "Chattogram", "Sylhet", "Khulna"]
def plan(i): return "gold" if i % 10 == 0 else "premium" if i % 10 in (1, 2) else "basic"

rows = [["Name", "Mobile", "E-mail", "City", "Plan", "Total Orders"]]
for i in range(1, 10001):          # existing identity users, matched by email
    rows.append([f"Existing {i}", None, f"user{i}@example.com", CITIES[i % 4], plan(i), i % 25])
for i in range(10001, 19991):      # new people; phone as an Excel NUMBER (leading 0 lost)
    rows.append([f"New {i}", 1700000000 + i, f"new{i}@shop.example.com", CITIES[i % 4], plan(i), i % 25])
for i in range(10):                # broken rows
    rows.append([f"Bad {i}", "123", "not-an-email", "Dhaka", "gold", 1])

sheet = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
         '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>']
for r, row in enumerate(rows, 1):
    sheet.append(f'<row r="{r}">' + "".join(cell(f"{col(c)}{r}", v) for c, v in enumerate(row, 1)) + "</row>")
sheet.append("</sheetData></worksheet>")

NS_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
with zipfile.ZipFile(sys.argv[1], "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '</Types>')
    z.writestr("_rels/.rels", f'<Relationships xmlns="{NS_REL}"><Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
    z.writestr("xl/workbook.xml", '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Customers" sheetId="1" r:id="rId1"/></sheets></workbook>')
    z.writestr("xl/_rels/workbook.xml.rels", f'<Relationships xmlns="{NS_REL}"><Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
    z.writestr("xl/worksheets/sheet1.xml", "\n".join(sheet))
print(f"{len(rows) - 1} data rows -> {sys.argv[1]}")
