#!/usr/bin/env python3
"""
Adiciona novas licitações sem duplicar dados existentes
"""

import psycopg2
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def adicionar_novas_licitacoes(json_file):
    """Adiciona apenas licitações novas ao banco"""

    print(f"📊 Adicionando novas licitações de: {json_file}")

    if not os.path.exists(json_file):
        print(f"❌ Arquivo não encontrado: {json_file}")
        return

    try:
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 5432),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            client_encoding='utf8'
        )

        cursor = conn.cursor()

        # Carregar dados do JSON
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        print(f"📋 Encontrados {len(json_data)} editais no JSON")

        # Obter PNCP_IDs já existentes no banco
        cursor.execute("SELECT pncp_id FROM tenders WHERE pncp_id IS NOT NULL")
        existing_ids = set(row[0] for row in cursor.fetchall())
        print(f"🔍 {len(existing_ids)} editais já existem no banco")

        # Filtrar apenas editais novos
        novos_editais = []
        for edital in json_data:
            pncp_id = edital.get('pncp_id', '')
            if pncp_id and pncp_id not in existing_ids:
                novos_editais.append(edital)

        print(f"🆕 {len(novos_editais)} editais novos para adicionar")

        if len(novos_editais) == 0:
            print("✅ Nenhum edital novo encontrado!")
            conn.close()
            return

        # Inserir apenas editais novos
        inseridos = 0
        for edital in novos_editais:
            try:
                # Preparar dados
                pncp_id = edital.get('pncp_id', '')
                title = edital.get('title', '')
                description = edital.get('description', '')
                organization_name = edital.get('organization_name', '')
                organization_cnpj = edital.get('organization_cnpj', '')
                municipality_name = edital.get('municipality_name', '')
                municipality_ibge = edital.get('municipality_ibge', '')
                state_code = edital.get('state_code', '')
                publication_date = edital.get('publication_date')
                status = edital.get('status', '')
                modality = edital.get('modality', '')
                estimated_value = edital.get('valor_total_estimado')
                source_url = edital.get('source_url', '')
                detail_url = edital.get('detail_url', '')
                data_source = f'PNCP_SCRAPING_{datetime.now().strftime("%Y%m%d")}'
                downloaded_files = json.dumps(edital.get('downloaded_files', []))
                created_at = datetime.now()

                # Tratar data de publicação
                if publication_date and isinstance(publication_date, str):
                    try:
                        publication_date = datetime.strptime(publication_date, '%Y-%m-%d').date()
                    except:
                        publication_date = None

                # Inserir no banco
                cursor.execute("""
                    INSERT INTO tenders (
                        pncp_id, title, description, organization_name, organization_cnpj,
                        municipality_name, municipality_ibge, state_code, publication_date,
                        status, modality, estimated_value, source_url, detail_url,
                        data_source, downloaded_files, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    pncp_id, title, description, organization_name, organization_cnpj,
                    municipality_name, municipality_ibge, state_code, publication_date,
                    status, modality, estimated_value, source_url, detail_url,
                    data_source, downloaded_files, created_at
                ))

                inseridos += 1
                print(f"✅ Adicionado: {title[:50]}...")

            except Exception as e:
                print(f"❌ Erro ao inserir edital: {e}")
                continue

        conn.commit()

        # Verificar total final
        cursor.execute("SELECT COUNT(*) FROM tenders")
        total_final = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT state_code) FROM tenders WHERE state_code IS NOT NULL")
        total_states = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT municipality_name) FROM tenders WHERE municipality_name IS NOT NULL")
        total_cities = cursor.fetchone()[0]

        conn.close()

        print(f"\\n🎉 Adição concluída!")
        print(f"📊 Estatísticas finais:")
        print(f"  - Editais novos adicionados: {inseridos}")
        print(f"  - Total de editais no banco: {total_final}")
        print(f"  - Estados: {total_states}")
        print(f"  - Cidades: {total_cities}")

    except Exception as e:
        print(f"❌ Erro: {e}")


def buscar_json_mais_recente():
    """Busca o arquivo JSON mais recente"""

    import glob

    # Procurar arquivos JSON
    patterns = [
        "editais_items_only_*.json",
        "upload/editais_items_only_*.json"
    ]

    json_files = []
    for pattern in patterns:
        json_files.extend(glob.glob(pattern))

    if not json_files:
        print("❌ Nenhum arquivo JSON encontrado!")
        return None

    # Pegar o mais recente
    json_file = max(json_files, key=os.path.getctime)
    print(f"📁 Arquivo JSON mais recente: {json_file}")

    return json_file


if __name__ == "__main__":
    print("🚀 Script para adicionar novas licitações")
    print("=" * 50)

    # Opção 1: Usar arquivo específico
    import sys

    if len(sys.argv) > 1:
        json_file = sys.argv[1]
        adicionar_novas_licitacoes(json_file)
    else:
        # Opção 2: Buscar arquivo mais recente
        json_file = buscar_json_mais_recente()
        if json_file:
            adicionar_novas_licitacoes(json_file)
        else:
            print("\\n📋 Como usar:")
            print("python adicionar_novas_licitacoes.py arquivo.json")
            print("ou")
            print("python adicionar_novas_licitacoes.py  # usa arquivo mais recente")
