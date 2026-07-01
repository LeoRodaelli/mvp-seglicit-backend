#!/usr/bin/env python3
"""
Atalho para testar reparo de licitações (dry-run por padrão).

Exemplos:
  python testar_reparo_licitacao.py --pncp-id "00394452000103-1-012349/2026"
  python testar_reparo_licitacao.py --limit 10
  python testar_reparo_licitacao.py --pncp-id "00394452000103-1-012349/2026" --apply
"""

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPAIR = SCRIPT_DIR / 'reparar_licitacoes_incompletas.py'


def main():
    argv = sys.argv[1:]
    if '--apply' not in argv and '--dry-run' not in argv:
        argv = ['--dry-run', '-v'] + argv
    elif '--apply' in argv and '--dry-run' not in argv:
        if '-v' not in argv and '--verbose' not in argv:
            argv = ['-v'] + argv

    cmd = [sys.executable, str(REPAIR)] + argv
    print('▶', ' '.join(cmd))
    raise SystemExit(subprocess.call(cmd, cwd=str(SCRIPT_DIR)))


if __name__ == '__main__':
    main()
