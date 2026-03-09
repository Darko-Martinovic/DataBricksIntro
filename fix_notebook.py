import json

def fix_mojibake(s):
    """Re-encode Windows-1252 mojibake back to proper UTF-8 characters."""
    try:
        return s.encode('cp1252').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s

nb = json.load(open(r'c:\Source\Dsa\DataBricks\databricks-project\src\Filtering DataFrame Records.ipynb', encoding='utf-8'))

for cell in nb['cells']:
    if cell['cell_type'] == 'markdown':
        cell['source'] = [fix_mojibake(line) for line in cell['source']]

json.dump(nb, open(r'c:\Source\Dsa\DataBricks\databricks-project\src\Filtering DataFrame Records.ipynb', 'w', encoding='utf-8'), indent=1, ensure_ascii=False)
print("Done.")

# Verify
nb2 = json.load(open(r'c:\Source\Dsa\DataBricks\databricks-project\src\Filtering DataFrame Records.ipynb', encoding='utf-8'))
for i, cell in enumerate(nb2['cells']):
    if cell['cell_type'] == 'markdown':
        src = ''.join(cell['source'])
        print(f"Cell {i}: {src[:60]}")
        print(f"  hex check: {[hex(ord(c)) for c in src[:6]]}")
