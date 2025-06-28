# crypto-dashboard

这是一份云计算与大数据技术的课程期末作业，目的是实现用binance接口监听价格动态。并使用 Kafka + Spark Streaming 实现实时流计算。

## 环境准备

- 操作系统：Linux (Ubuntu16.04)
- JDK 1.8.0_162
- Python 3.8+ (3.8.5)
- Kafka 2.12-2.6.0（建议安装在 /usr/local/kafka）
- Spark 3.2.0 （建议安装在 /usr/local/spark）
- Kafka 和 Spark 连接用的 jar 包放入 Spark 路径中

## 安装依赖

```bash
pip install -r requirements.txt
```

下载 jar 包放入 Spark 目录
- spark-token-provider-kafka-0-10_2.12-3.2.0.jar
- spark-streaming-kafka-0-10_2.12-3.2.0.jar
- spark-streaming-kafka-0-8_2.11-2.4.0.jar
- spark-sql-kafka-0-10_2.12-3.2.0.jar
- commons-pool2-2.9.0.jar
- kafka-clients-2.6.0.jar

## 启动前的准备工作

此处命令假设你的kafka和spark都安装在 /usr/local 目录下，请根据实际安装路径调整。

1. **启动 Zookeeper 服务** 
   这里使用前台启动
   ```bash
   cd /usr/local/kafka
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```

2. **启动 Kafka 服务**
   ```bash
   cd /usr/local/kafka
   bin/kafka-server-start.sh config/server.properties
   ```

3. **创建 Kafka Topic**  
   ```bash
   /usr/local/kafka/bin/kafka-topics.sh --create --topic btc_price --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   /usr/local/kafka/bin/kafka-topics.sh --create --topic btc_alert --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

4. **启动 Spark 服务**（如需使用 Spark Standalone 模式）
   ```bash
   /usr/local/spark/sbin/start-all.sh
   ```

## 启动流程

1. **启动币安数据采集（生产者）**
   ```bash
   python binance_producer.py
   ```

2. **启动 Spark Streaming 实时计算（记得改spark_streaming.py的路径）**
   ```bash
   /usr/local/spark/bin/spark-submit \
   --master local[2] \
   --conf spark.hadoop.fs.defaultFS=file:/// \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:2.6.0 \
   /home/hadoop/pythonworkspace/crypto-dashboard/crypto-dashboard/spark_streaming.py
   ```

3. **启动 Web 服务**
   ```bash
   python app.py
   ```

4. **启动告警消费者**
   ```bash
   python consumer.py
   ```

5. **访问前端页面**  
   在浏览器中打开 [http://localhost:5000](http://localhost:5000)

---
