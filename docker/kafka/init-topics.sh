#!/bin/bash
# =============================================================================
# init-topics.sh
# Скрипт для создания топиков Kafka вручную (если не используется init-контейнер)
# Запускать из контейнера Kafka или с машины, где установлен Kafka CLI
#
# Использование:
#   docker exec -it marketdata-kafka bash -c "$(cat docker/kafka/init-topics.sh)"
#   или
#   cat docker/kafka/init-topics.sh | docker exec -i marketdata-kafka bash
#
# Для внешнего подключения (с хост-машины):
#   docker/kafka/init-topics.sh localhost:9094
# =============================================================================

BOOTSTRAP_SERVER="${1:-localhost:9092}"
KAFKA_HOME="/opt/kafka"
KAFKA_TOPICS="${KAFKA_HOME}/bin/kafka-topics.sh"

echo "Using bootstrap server: $BOOTSTRAP_SERVER"
echo "Kafka topics CLI: $KAFKA_TOPICS"
echo "Creating topics..."

# Топик для сырых тиков (3 партиции — параллельная запись по символам)
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
  --topic raw-ticks \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000   # 7 дней

# Топик для агрегированных данных (3 партиции)
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
  --topic aggregated-data \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000  # 30 дней

# Топик для событий подключений (1 партиция — порядок важен)
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
  --topic connection-events \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=86400000    # 1 день

echo ""
echo "Topics created. Listing all topics:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list

echo ""
echo "Detailed info for topic 'raw-ticks':"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic raw-ticks
