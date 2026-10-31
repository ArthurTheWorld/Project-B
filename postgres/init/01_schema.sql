-- Schema transacional (OLTP) simulado.
-- O Debezium lê o WAL lógico: toda alteração aqui vira evento no Kafka.

create schema if not exists loja;

create table if not exists loja.pedidos (
    pedido_id     bigint primary key,
    cliente_id    bigint       not null,
    status        text         not null default 'novo',
    valor         numeric(12,2) not null,
    canal         text,
    criado_em     timestamptz  not null default now(),
    atualizado_em timestamptz  not null default now()
);

-- Sem REPLICA IDENTITY FULL, updates/deletes não trazem as colunas antigas
-- no evento — e o consumidor perde a capacidade de fazer merge por chave.
alter table loja.pedidos replica identity full;

create index if not exists idx_pedidos_status on loja.pedidos (status);
