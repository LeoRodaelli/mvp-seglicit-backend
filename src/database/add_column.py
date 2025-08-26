import sqlite3

# Caminho até o banco
conn = sqlite3.connect("app.db")
cursor = conn.cursor()

# Adicionar a nova coluna, se ainda não existir
try:
    cursor.execute("ALTER TABLE tenders ADD COLUMN downloaded_files TEXT;")
    print("Coluna 'downloaded_files' adicionada com sucesso.")
except sqlite3.OperationalError as e:
    print("Erro:", e)

conn.commit()
conn.close()
