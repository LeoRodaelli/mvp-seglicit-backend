#!/usr/bin/env python3
"""
Limpa banco PostgreSQL e repovoa com dados do JSON
"""

import psycopg2
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def limpar_e_repovoar():
    """Limpa banco e repovoa com dados limpos"""

    print("🧹 Limpando e repovando banco PostgreSQL...")

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

        # 1. LIMPAR TABELA
        print("🗑️  Limpando tabela tenders...")
        cursor.execute("DELETE FROM tenders")
        conn.commit()
        print("✅ Tabela limpa!")

        # 2. CARREGAR DADOS DO JSON
        json_files = [
            "editais_items_only_20250820_142850.json",
            "upload/editais_items_only_20250820_142850.json"
        ]

        json_data = None
        for json_file in json_files:
            if os.path.exists(json_file):
                print(f"📁 Carregando dados de: {json_file}")
                with open(json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                break

        if not json_data:
            print("❌ Arquivo JSON não encontrado!")
            return

        print(f"📊 Encontrados {len(json_data)} editais no JSON")

        # 3. INSERIR DADOS LIMPOS
        inseridos = 0
        for edital in json_data:
            try:
                # Preparar dados com encoding correto
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
                data_source = 'PNCP_SCRAPING_JSON'
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
                print(f"✅ Inserido: {title[:50]}...")

            except Exception as e:
                print(f"❌ Erro ao inserir edital: {e}")
                continue

        conn.commit()

        # 4. VERIFICAR DADOS INSERIDOS
        cursor.execute("SELECT COUNT(*) FROM tenders")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT title, municipality_name FROM tenders LIMIT 3")
        examples = cursor.fetchall()

        print(f"\\n🎉 Repovamento concluído!")
        print(f"📊 Total de editais: {total}")
        print(f"✅ Editais inseridos: {inseridos}")

        print("\\n📋 Exemplos de dados inseridos:")
        for title, city in examples:
            print(f"  - {title[:50]}... | {city}")

        # 5. TESTAR QUERIES
        print("\\n🔍 Testando queries...")

        # Testar estados
        cursor.execute("SELECT state_code, COUNT(*) FROM tenders GROUP BY state_code")
        states = cursor.fetchall()
        print(f"✅ Estados: {len(states)}")
        for state, count in states:
            print(f"  - {state}: {count} licitações")

        # Testar cidades
        cursor.execute("SELECT municipality_name, COUNT(*) FROM tenders GROUP BY municipality_name LIMIT 5")
        cities = cursor.fetchall()
        print(f"✅ Cidades (primeiras 5): {len(cities)}")
        for city, count in cities:
            print(f"  - {city}: {count} licitações")

        conn.close()

        print("\\n🎯 Banco repovado com sucesso!")
        print("Agora reinicie o backend e teste!")

    except Exception as e:
        print(f"❌ Erro: {e}")


if __name__ == "__main__":
    limpar_e_repovoar()
