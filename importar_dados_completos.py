#!/usr/bin/env python3
"""
Importa dados completos com todos os campos
"""

import psycopg2
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def importar_dados_completos(json_file):
    """Importa dados com todos os campos novos"""

    print(f"📊 Importando dados completos de: {json_file}")

    if not os.path.exists(json_file):
        print(f"❌ Arquivo não encontrado: {json_file}")
        return

    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 5432),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            client_encoding='utf8'
        )

        cursor = conn.cursor()

        # Carregar JSON
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        print(f"📋 Encontrados {len(json_data)} editais no JSON")

        # Obter IDs existentes
        cursor.execute("SELECT pncp_id FROM tenders WHERE pncp_id IS NOT NULL")
        existing_ids = set(row[0] for row in cursor.fetchall())

        # Processar editais
        inseridos = 0
        atualizados = 0

        for edital in json_data:
            pncp_id = edital.get('pncp_id', '')

            # Preparar dados completos
            dados = {
                'pncp_id': pncp_id,
                'title': edital.get('title', ''),
                'description': edital.get('description', ''),
                'organization_name': edital.get('organization_name', ''),
                'organization_cnpj': edital.get('organization_cnpj', ''),
                'municipality_name': edital.get('municipality_name', ''),
                'municipality_ibge': edital.get('municipality_ibge', ''),
                'state_code': edital.get('state_code', ''),
                'status': edital.get('status', ''),
                'modality': edital.get('modality', ''),
                'estimated_value': edital.get('estimated_value'),
                'source_url': edital.get('source_url', ''),
                'detail_url': edital.get('detail_url', ''),
                'data_source': f'PNCP_SCRAPING_{datetime.now().strftime("%Y%m%d")}',
                'created_at': datetime.now(),
                # NOVOS CAMPOS
                'objeto': edital.get('objeto', ''),
                'items_json': json.dumps(edital.get('items', []), ensure_ascii=False),
                'downloaded_files_json': json.dumps(edital.get('downloaded_files', []), ensure_ascii=False),
                'prazo': edital.get('prazo', ''),
                'detailed_description': edital.get('detailed_description', ''),
                'valor_total_estimado': edital.get('valor_total_estimado'),
                'items_count': len(edital.get('items', [])),
                'downloads_count': len(edital.get('downloaded_files', []))
            }

            # Tratar data de publicação
            publication_date = edital.get('publication_date')
            if publication_date and isinstance(publication_date, str):
                try:
                    dados['publication_date'] = datetime.strptime(publication_date, '%Y-%m-%d').date()
                except:
                    dados['publication_date'] = None
            else:
                dados['publication_date'] = None

            try:
                if pncp_id in existing_ids:
                    # Atualizar registro existente
                    cursor.execute("""
                        UPDATE tenders SET
                            title = %(title)s,
                            description = %(description)s,
                            organization_name = %(organization_name)s,
                            municipality_name = %(municipality_name)s,
                            state_code = %(state_code)s,
                            publication_date = %(publication_date)s,
                            status = %(status)s,
                            modality = %(modality)s,
                            estimated_value = %(estimated_value)s,
                            source_url = %(source_url)s,
                            detail_url = %(detail_url)s,
                            objeto = %(objeto)s,
                            items_json = %(items_json)s,
                            downloaded_files_json = %(downloaded_files_json)s,
                            prazo = %(prazo)s,
                            detailed_description = %(detailed_description)s,
                            valor_total_estimado = %(valor_total_estimado)s,
                            items_count = %(items_count)s,
                            downloads_count = %(downloads_count)s
                        WHERE pncp_id = %(pncp_id)s
                    """, dados)
                    atualizados += 1
                    print(f"🔄 Atualizado: {dados['title'][:50]}...")
                else:
                    # Inserir novo registro
                    cursor.execute("""
                        INSERT INTO tenders (
                            pncp_id, title, description, organization_name, organization_cnpj,
                            municipality_name, municipality_ibge, state_code, publication_date,
                            status, modality, estimated_value, source_url, detail_url,
                            data_source, created_at, objeto, items_json, downloaded_files_json,
                            prazo, detailed_description, valor_total_estimado, items_count, downloads_count
                        ) VALUES (
                            %(pncp_id)s, %(title)s, %(description)s, %(organization_name)s, %(organization_cnpj)s,
                            %(municipality_name)s, %(municipality_ibge)s, %(state_code)s, %(publication_date)s,
                            %(status)s, %(modality)s, %(estimated_value)s, %(source_url)s, %(detail_url)s,
                            %(data_source)s, %(created_at)s, %(objeto)s, %(items_json)s, %(downloaded_files_json)s,
                            %(prazo)s, %(detailed_description)s, %(valor_total_estimado)s, %(items_count)s, %(downloads_count)s
                        )
                    """, dados)
                    inseridos += 1
                    print(f"✅ Inserido: {dados['title'][:50]}...")

            except Exception as e:
                print(f"❌ Erro ao processar {pncp_id}: {e}")
                continue

        conn.commit()
        conn.close()

        print(f"\\n🎉 Importação concluída!")
        print(f"📊 Novos editais: {inseridos}")
        print(f"🔄 Editais atualizados: {atualizados}")

    except Exception as e:
        print(f"❌ Erro: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        json_file = sys.argv[1]
    else:
        json_file = "editais_items_only_20250826_205652.json"

    importar_dados_completos(json_file)
