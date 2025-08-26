import json
import sqlite3
from pathlib import Path

# Caminho do seu JSON mais recente
json_file = "editais_items_only_20250820_142850.json"  # Ajuste o nome
db_file = "src/database/app.db"

# Ler JSON
with open(json_file, 'r', encoding='utf-8') as f:
    editais = json.load(f)

# Conectar ao banco
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Atualizar registros existentes com detail_url
for edital in editais:
    pncp_id = edital.get('pncp_id')
    detail_url = edital.get('detail_url')

    if pncp_id and detail_url:
        cursor.execute(
            "UPDATE tenders SET detail_url = ? WHERE pncp_id = ?",
            (detail_url, pncp_id)
        )
        print(f"✅ Atualizado: {edital.get('title', 'N/A')[:50]}...")

conn.commit()
conn.close()
print("✅ Importação concluída!")
