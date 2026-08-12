# nexuspay-transaction-worker

Consumidor da fila SQS FIFO do NexusPay. Executa o movimento de saldo com
garantias ACID e grava a auditoria. **Não tem API de negócio** — a única porta
HTTP é `/actuator/health`, que o gateway usa contra o scale-to-zero.

Java 25, Spring Boot 4.0.7, Spring Cloud AWS 4.1.0, acesso a dados por
`JdbcClient`.

## Rodar

Pré-requisitos: Docker ligado, e o Postgres do gateway no ar em `localhost:5433`
com `alembic upgrade head` aplicado.

```bash
./mvnw spring-boot:run
```

## Testes

```bash
./mvnw test
```

A suíte sobe PostgreSQL 16 e LocalStack em containers descartáveis. **Nenhum
teste automatizado fala com a fila real.**

O teste de fumaça contra a AWS de verdade fica desligado por padrão e só lê
atributos — não publica nem consome:

```bash
./mvnw test -Dtest=RealQueueSmokeTest -Dfila.real=true
```

> **A fila é uma só, compartilhada entre desenvolvimento e produção.** Nunca
> aponte este worker para ela com um banco de desenvolvimento a menos que
> entenda a consequência: uma mensagem de produção consumida aqui não acha o
> UUID no banco local, é descartada como no-op terminal, e a transação real
> fica presa em `PENDING` até a varredura do gateway republicar.

## Schema

Este worker **não roda migrations**. O Alembic no repositório
`nexuspay-api-gateway` é dono único do schema.

Os testes usam um dump versionado em `src/test/resources/schema.sql`. Depois de
qualquer migration nova no gateway que toque `accounts`, `transactions` ou
`ledger_entries`:

```bash
bash scripts/regenerate-schema.sh
```

`SchemaDriftTest` falha se uma coluna de que o worker depende desaparecer.

## Credenciais

Perfil nomeado em `~/.aws/credentials`, nunca no código nem em `.env`:

```ini
[nexuspay-worker]
aws_access_key_id = ...
aws_secret_access_key = ...
```

A policy desse usuário precisa de exatamente cinco ações sobre o ARN da fila
principal: `ReceiveMessage`, `DeleteMessage`, `GetQueueAttributes`,
`GetQueueUrl`, `ChangeMessageVisibility`. Nada sobre a DLQ — quem move a
mensagem é a própria SQS.
