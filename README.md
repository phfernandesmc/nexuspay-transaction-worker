# nexuspay-transaction-worker
Consumer microservice built with Java 21 &amp; Spring Boot. Polls AWS SQS queues to process financial transactions in the background. Guarantees ACID data consistency in PostgreSQL, preventing race conditions and ensuring secure balance updates.
