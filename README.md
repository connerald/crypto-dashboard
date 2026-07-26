# Crypto Dashboard

一个持续演进的加密资产行情与实时数据处理项目。

当前代码保留了原型阶段的核心数据链路：

- 从 Binance 公共接口采集 BTC/USDT 行情
- 通过 Kafka 传递价格、涨跌幅与告警消息
- 使用 Spark Structured Streaming 进行窗口计算
- 使用 Flask 和 Socket.IO 展示实时数据

## 支持环境

- Docker Engine 或 Docker Desktop
- Docker Compose
- 能够拉取 Docker 镜像、Python 包和 Maven 依赖的网络

项目只支持 Linux 应用运行环境。应用容器固定使用 Ubuntu 22.04、Python 3.12 和 Java 17；Kafka 固定使用官方 `apache/kafka:3.9.2` 镜像。

macOS 可以作为 Docker Desktop 宿主机，但不支持在 macOS 上原生启动应用。宿主机不需要安装 Java、Kafka、Spark、Python 或 uv。

首次构建和启动时，Docker 会自动获取：

- `ubuntu:22.04`
- `apache/kafka:3.9.2`
- `ghcr.io/astral-sh/uv:0.11.23`

Spark 首次启动时还会从 Maven 仓库下载与 PySpark 3.5.9 对应的 Kafka Connector，随后缓存在 Docker 命名卷中。

## 启动项目

从项目根目录构建并在后台启动完整数据链路：

```bash
docker compose up --build --detach
```

Compose 会按依赖顺序启动以下服务：

| 服务 | 作用 |
| --- | --- |
| `kafka-volume-init` | 初始化 Kafka 命名卷权限后退出 |
| `kafka` | 单节点 KRaft Kafka Broker |
| `kafka-init` | 幂等创建 `btc_price`、`btc_alert`、`btc_change` |
| `web` | Flask-SocketIO 页面 |
| `consumer` | 将 Kafka 消息转发给 Socket.IO |
| `spark` | 计算一分钟窗口涨跌幅和告警 |
| `producer` | 从 Binance 获取 BTC/USDT 行情 |

查看状态和日志：

```bash
docker compose ps
docker compose logs --follow
```

访问 [http://localhost:5000](http://localhost:5000)。只有 Web 的 `127.0.0.1:5000` 会映射到宿主机；Kafka 仅能通过 Compose 网络中的 `kafka:9092` 访问。

页面使用 Socket.IO 4.x 客户端，服务端使用 Flask-SocketIO 5.x；升级或部署时应保持两端协议版本兼容并一同发布。

## 日常开发

源码以只读方式挂载到应用容器。修改代码后重启受影响的服务即可，例如：

```bash
docker compose restart web consumer spark producer
```

修改 `pyproject.toml` 或 `uv.lock` 后需要重新构建应用镜像：

```bash
docker compose build
docker compose up --detach
```

查看 Kafka Topic：

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

停止并移除容器和网络，但保留 Kafka 数据、Spark checkpoint 和 Connector 缓存：

```bash
docker compose down
```

同时永久删除这些 Docker 命名卷：

```bash
docker compose down --volumes
```

`--volumes` 会删除本地 Kafka 消息和流处理状态，不能恢复。

## 依赖管理

`pyproject.toml` 声明项目直接依赖的兼容范围，`uv.lock` 固定 Linux `x86_64` 和 `aarch64` 的完整依赖图。锁文件必须提交到版本控制，不能手工编辑。

在已构建的 Linux 应用容器中检查锁文件：

```bash
docker compose run --rm --no-deps web uv lock --check
```

增加、删除或升级依赖时使用固定的 uv 0.11.x 重新生成锁文件，并同时提交 `pyproject.toml` 和 `uv.lock`。Docker 构建使用 `uv sync --locked`，锁文件与声明不一致时会直接失败。

仓库中已有的宿主机 `.venv` 不再参与运行，可以保留，也可以由用户自行删除；Compose 不会读取或修改它。

## 历史课程作业

旧课程作业已经冻结，不会随 `main` 的后续开发发生变化：

- [浏览归档源码（2025-06-28）](https://github.com/connerald/crypto-dashboard/tree/archive/coursework-final-2025-06-28)
- 归档标签：`archive/coursework-final-2025-06-28`
- 归档提交：`712b0efee816725684e7b12b22d654c69a8c6049`

原始环境要求、依赖安装和启动说明均保留在归档版本的 README 中。

## 开发约定

- `main` 是默认且唯一的长期开发分支。
- 历史作业通过不可变标签保存，不复制到新版源码目录。
- 文本文件统一使用 LF 行尾。
- Python 依赖仅通过 uv 管理，不维护单独的 `requirements.txt`。
- 支持的应用运行平台只有 Linux；macOS 仅作为 Docker 宿主机。
- 每次推送应保持提交目的明确，并同步更新受影响的说明。
