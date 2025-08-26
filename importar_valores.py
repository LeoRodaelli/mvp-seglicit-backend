import json
import sqlite3
from pathlib import Path

# Caminho do seu JSON mais recente
json_file = "editais_items_only_20250820_142850.json"  # Ajuste o nome se necessário
db_file = "src/database/app.db"

# Ler JSON
with open(json_file, 'r', encoding='utf-8') as f:
    editais = json.load(f)

# Conectar ao banco
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Atualizar registros existentes com valor_total_estimado
atualizados = 0
for edital in editais:
    pncp_id = edital.get('pncp_id')
    valor_total = edital.get('valor_total_estimado')

    if pncp_id and valor_total is not None:
        cursor.execute(
            "UPDATE tenders SET estimated_value = ? WHERE pncp_id = ?",
            (valor_total, pncp_id)
        )
        atualizados += 1
        print(f"✅ Valor R$ {valor_total:,.2f} - {edital.get('title', 'N/A')[:50]}...")

conn.commit()
conn.close()
print(f"✅ {atualizados} valores atualizados!")
