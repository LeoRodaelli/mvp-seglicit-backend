# Known Bugs

Problemas conhecidos e seu status.

## Bugs Conhecidos
O agente Zaia da plataforma estava exibindo a string literal `@response.resultado` em vez dos dados reais.

## Onde Acontecem
Na interface de chat da plataforma Zaia (Agente ID 76034).

## Hipóteses de Causa
O nome da ação configurada na Zaia continha caracteres especiais (`Buscar_Licitações_Seglicit`), o que pode causar falhas silenciosas na execução da chamada de API pela plataforma Zaia.

## Tentativas Já Feitas
O endpoint foi simplificado, erros de SQL foram corrigidos, e instruções foram fornecidas para renomear a ação na plataforma Zaia removendo caracteres especiais.
