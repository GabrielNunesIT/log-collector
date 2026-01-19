# Multi-Ingestion & Multi-Emission Example

This example demonstrates the **Log Collector** handling multiple sources and destinations simultaneously using Docker Compose.

## 🏗️ Architecture

- **Ingestors**:
  - **Syslog UDP** (`:5140`)
  - **Syslog TCP** (`:5140`)
  - **File Tail** (`/var/log/input/*.log` from shared volume)
- **Emitters**:
  - **Elasticsearch** (`http://elasticsearch:9200`)
  - **Loki** (`http://loki:3100`)
  - **Stdout** (Container logs)

## 🚀 How to Run

1. **Start the Stack**:

   ```bash
   docker-compose up --build
   ```

2. **Verify File Ingestion**:
   The `log-generator` service automatically writes logs to a shared volume. You should see these immediately in the `log-collector` console output (Stdout emitter).

3. **Verify Syslog Ingestion**:
   Send a manual syslog message via Netcat/Telnet:

   **UDP**:

   ```bash
   echo "<13>Jan 1 00:00:00 localhost test-udp: Hello UDP" | nc -u -w 1 localhost 5140
   ```

4. **Verify Emissions**:

   - **Elasticsearch**:

     ```bash
     curl "http://localhost:9201/collector-logs/_search?pretty"
     ```

   - **Loki**:

     ```bash
     curl "http://localhost:3100/loki/api/v1/query?query={job=\"log-collector\"}"
     ```

   - **Grafana**:
     access **<http://localhost:3000>** (No login required).
     Go to **Explore** and select **Loki** datasource.
     Query: `{job="log-collector"}`

   - **Kibana**:
     access **<http://localhost:5602>** (No login required).
     1. Go to **Stack Management** > **Data Views**.
     2. Create data view for index pattern `collector-logs*`.
     3. Go to **Discover** to see logs.

   - **VictoriaLogs**:
     access **<http://localhost:9428/select/vmui>**
     Query: `_stream_id: "log-collector"` or simple `*`

## 🧹 Cleanup

```bash
docker-compose down -v
```
