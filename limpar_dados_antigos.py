#!/usr/bin/env python3
"""
Script para limpar dados antigos incompletos do banco de dados
Remove apenas licitações que NÃO têm objeto, items_json ou valor_total_estimado
"""

import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def limpar_dados_antigos(modo='preview'):
    """
    Limpa dados antigos incompletos
    
    Args:
        modo: 'preview' (mostra o que será deletado) ou 'delete' (deleta de verdade)
    """
    
    print("🧹 Script de Limpeza de Dados Antigos")
    print("=" * 60)
    
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
        
        # Estatísticas ANTES
        print("\n📊 Estatísticas ANTES da limpeza:")
        cursor.execute("SELECT COUNT(*) FROM tenders")
        total_antes = cursor.fetchone()[0]
        print(f"  Total de licitações: {total_antes}")
        
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE objeto IS NOT NULL AND objeto != ''")
        com_objeto = cursor.fetchone()[0]
        print(f"  Com objeto: {com_objeto}")
        
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE items_json IS NOT NULL")
        com_items = cursor.fetchone()[0]
        print(f"  Com items: {com_items}")
        
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE valor_total_estimado IS NOT NULL")
        com_valor = cursor.fetchone()[0]
        print(f"  Com valor: {com_valor}")
        
        # Identificar licitações incompletas
        print("\n🔍 Identificando licitações incompletas...")
        cursor.execute("""
            SELECT 
                id, 
                pncp_id, 
                title,
                created_at,
                CASE WHEN objeto IS NULL OR objeto = '' THEN 'NÃO' ELSE 'SIM' END as tem_objeto,
                CASE WHEN items_json IS NULL THEN 'NÃO' ELSE 'SIM' END as tem_items,
                CASE WHEN valor_total_estimado IS NULL THEN 'NÃO' ELSE 'SIM' END as tem_valor
            FROM tenders
            WHERE 
                (objeto IS NULL OR objeto = '')
                OR items_json IS NULL
                OR valor_total_estimado IS NULL
            ORDER BY id
        """)
        
        incompletas = cursor.fetchall()
        print(f"  Encontradas: {len(incompletas)} licitações incompletas")
        
        if len(incompletas) == 0:
            print("\n✅ Nenhuma licitação incompleta encontrada!")
            conn.close()
            return
        
        # Mostrar exemplos
        print("\n📋 Exemplos de licitações incompletas (primeiras 10):")
        print("-" * 100)
        print(f"{'ID':<6} {'PNCP ID':<30} {'Título':<40} {'Objeto':<8} {'Items':<8} {'Valor':<8}")
        print("-" * 100)
        
        for i, row in enumerate(incompletas[:10]):
            id_val, pncp_id, title, created_at, tem_objeto, tem_items, tem_valor = row
            title_short = title[:37] + "..." if len(title) > 40 else title
            print(f"{id_val:<6} {pncp_id:<30} {title_short:<40} {tem_objeto:<8} {tem_items:<8} {tem_valor:<8}")
        
        if len(incompletas) > 10:
            print(f"... e mais {len(incompletas) - 10} licitações")
        
        # Modo PREVIEW
        if modo == 'preview':
            print("\n⚠️  MODO PREVIEW - Nenhum dado foi deletado!")
            print("\n📝 Para deletar de verdade, execute:")
            print("   python limpar_dados_antigos.py delete")
            
            # Mostrar o que seria deletado
            print(f"\n🗑️  Seriam deletadas: {len(incompletas)} licitações")
            print(f"   Restariam: {total_antes - len(incompletas)} licitações")
            
        # Modo DELETE
        elif modo == 'delete':
            print("\n⚠️  ATENÇÃO: Você está prestes a deletar dados!")
            print(f"   Serão deletadas: {len(incompletas)} licitações")
            print(f"   Restarão: {total_antes - len(incompletas)} licitações")
            
            resposta = input("\n❓ Tem certeza? Digite 'SIM' para confirmar: ")
            
            if resposta.strip().upper() != 'SIM':
                print("\n❌ Operação cancelada!")
                conn.close()
                return
            
            # Fazer backup antes de deletar
            print("\n💾 Criando backup...")
            backup_file = f"backup_antes_limpeza_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
            
            # Exportar IDs que serão deletados
            ids_para_deletar = [str(row[0]) for row in incompletas]
            
            with open(backup_file.replace('.sql', '_ids.txt'), 'w') as f:
                f.write('\n'.join(ids_para_deletar))
            
            print(f"   Backup dos IDs salvo em: {backup_file.replace('.sql', '_ids.txt')}")
            
            # Deletar
            print("\n🗑️  Deletando licitações incompletas...")
            cursor.execute("""
                DELETE FROM tenders
                WHERE 
                    (objeto IS NULL OR objeto = '')
                    OR items_json IS NULL
                    OR valor_total_estimado IS NULL
            """)
            
            deletados = cursor.rowcount
            conn.commit()
            
            print(f"   ✅ Deletados: {deletados} registros")
            
            # Estatísticas DEPOIS
            print("\n📊 Estatísticas DEPOIS da limpeza:")
            cursor.execute("SELECT COUNT(*) FROM tenders")
            total_depois = cursor.fetchone()[0]
            print(f"  Total de licitações: {total_depois}")
            
            cursor.execute("SELECT COUNT(*) FROM tenders WHERE objeto IS NOT NULL AND objeto != ''")
            com_objeto_depois = cursor.fetchone()[0]
            print(f"  Com objeto: {com_objeto_depois}")
            
            cursor.execute("SELECT COUNT(*) FROM tenders WHERE items_json IS NOT NULL")
            com_items_depois = cursor.fetchone()[0]
            print(f"  Com items: {com_items_depois}")
            
            cursor.execute("SELECT COUNT(*) FROM tenders WHERE valor_total_estimado IS NOT NULL")
            com_valor_depois = cursor.fetchone()[0]
            print(f"  Com valor: {com_valor_depois}")
            
            print(f"\n🎉 Limpeza concluída!")
            print(f"   Antes: {total_antes} licitações")
            print(f"   Depois: {total_depois} licitações")
            print(f"   Removidas: {total_antes - total_depois} licitações")
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys
    
    # Verificar modo
    modo = 'preview'
    if len(sys.argv) > 1:
        if sys.argv[1].lower() in ['delete', 'deletar', 'apagar']:
            modo = 'delete'
    
    limpar_dados_antigos(modo)
