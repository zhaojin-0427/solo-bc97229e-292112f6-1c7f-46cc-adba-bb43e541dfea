# 乱序双端 FASTQ 样本入湖服务

一个可直接启动的 Flask + Gunicorn + SQLite 服务。测序中心可通过浏览器上传样本号和两个 gzip FASTQ 文件，服务将乱序的 R1/R2 记录按 **R1 原始输入顺序**重排为严格同步的两个 gzip 文件，并原子发布样本包。

## 已实现的核心约束

- 每条 FASTQ 记录必须恰好四行：标题、序列、`+` 分隔行、质量行。
- 标题去掉开头 `@` 后，取第一个空白字符前的内容作为 ID。
- ID 末尾的 `/1` 或 `/2` 会被去掉，再用于 R1/R2 配对。
- 每条记录的序列长度和质量长度必须相等。
- R1 内配对 ID 唯一，R2 内配对 ID 唯一。
- R1 与 R2 的配对 ID 集合必须完全相同。
- 输入必须是完整、可解压且 CRC 正确的 gzip 文件；末尾才发现的 CRC/EOF 错误也会导致失败。
- 任何校验失败都不会发布部分结果，也不会替换已发布旧包。
- 重新上传同一样本时，旧包在整个处理期间可下载；只有两个新文件全部写完、复读并校验成功后，才通过原子 symlink 切换发布。
- 进程重启后：
  - 已发布包仍在 `data/packages/<sample>/current/` 下，可立即下载；
  - 未完成任务标记为失败；
  - 未完成 release 和暂存文件被清理；
  - 数据库目录从磁盘上的已发布包重新同步。
- 处理内存不随总读数线性增长。记录写入磁盘暂存，索引存放在每任务独立 SQLite 数据库中；进程内只保留当前 FASTQ 记录和少量状态。

## 快速启动

```bash
docker compose up --build
```

打开：

```text
http://localhost:5000/
```

使用不同宿主机端口：

```bash
HOST_PORT=8080 docker compose up --build
```

停止：

```bash
docker compose down
```

数据保存在 Docker volume `sample-ingest-data` 中；执行 `docker compose down -v` 才会删除数据。

## 浏览器使用流程

1. 打开首页，输入样本号。
2. 分别选择 R1、R2 的 `.fastq.gz` 文件。
3. 提交后页面显示：
   - 上传进度；
   - R1/R2 读取记录数；
   - 当前阶段和百分比；
   - 失败原因。
4. 成功后在首页或任务页下载：
   - 样本 ZIP；
   - 单独的 R1 gzip；
   - 单独的 R2 gzip；
   - manifest JSON。
5. 若同一样本重新上传失败，页面会继续展示旧版本下载链接。

## 示例数据

示例位于 `examples/`，可直接通过网页上传。

| 目录 | 用途 | 预期结果 |
|---|---|---|
| `valid-disordered/` | R1/R2 顺序完全不同的有效数据 | 成功，输出按 R1 原顺序同步排列 |
| `tail-missing-pair/` | R2 末尾缺一条配对 | 扫到末尾才发现缺配，失败且不发布 |
| `tail-corrupt-gzip/` | R2 三条记录可读出，但 gzip 尾部 CRC 被改坏 | 读到末尾才发现 CRC 错误，失败且不发布 |

重新生成示例：

```bash
python3 tools/make_examples.py
```

## 输出包结构

ZIP 文件名：`<sample_id>_package.zip`

内容示例：

```text
SAMPLE-001_R1.fastq.gz
SAMPLE-001_R2.fastq.gz
manifest.json
```

manifest 包含样本号、任务 ID、release ID、记录数、两个 gzip 的 SHA-256、大小和归档条目。磁盘上的 release 目录还包含：

```text
data/packages/<sample_id>/
├── current -> releases/<release_id>
└── releases/
    ├── <old_release_id>/
    │   ├── R1.fastq.gz
    │   ├── R2.fastq.gz
    │   ├── manifest.json
    │   ├── package.zip
    │   └── .complete
    └── <new_release_id>/
        └── ...
```

`current` 是最后一步原子替换的 symlink。替换前旧 release 不移动、不删除，因此下载旧包不会被处理中的新任务影响。默认保留当前版本和上一版本，可通过 `KEEP_RELEASES` 调整。

## 处理算法

### 1. 流式校验 R1，建立 SQLite 索引

逐记录读取 R1 gzip：

1. 校验四行结构。
2. 校验序列/质量长度。
3. 解析配对 ID。
4. 检查 R1 内 ID 唯一。
5. 在任务级 SQLite 中保存 `pair_id -> R1 顺序号`。

### 2. 流式校验 R2，建立偏移索引

逐记录读取 R2 gzip：

- 同样执行结构和长度校验；
- 检查 R2 内 ID 唯一；
- 将完整 R2 记录追加到磁盘 spool；
- SQLite 保存 `pair_id -> spool offset/length`。

### 3. 集合校验

用 SQLite 连接检查两端集合：

- R1 存在但 R2 不存在的 ID；
- R2 存在但 R1 不存在的 ID；
- 两端记录数是否一致。

### 4. 按 R1 原顺序写出

再次流式读取 R1，以 R1 顺序逐条：

1. 用配对 ID 从 SQLite 查询 R2 spool 偏移；
2. 读取对应 R2 记录；
3. 同时写入新的 R1/R2 gzip。

### 5. 发布前复读校验

两个输出文件会再次完整解压：

- 检查 gzip CRC/EOF；
- 检查 FASTQ 结构；
- 检查两端记录数相同；
- 检查第 N 条 R1 与第 N 条 R2 的配对 ID 一致。

同时对写入和复读的压缩字节计算 SHA-256，确认磁盘内容一致。全部成功后才进入发布流程。

## HTTP 接口

### 上传

```http
POST /samples
Content-Type: multipart/form-data
```

表单字段：

- `sample_id`
- `r1`
- `r2`

浏览器表单会重定向到任务页；AJAX 请求（带 `Accept: application/json`）返回：

```json
{
  "id": "...",
  "sample_id": "...",
  "status_url": "/api/jobs/...",
  "job_url": "/jobs/..."
}
```

### 查询任务

```http
GET /api/jobs/<job_id>
```

### 下载

```text
GET /samples/<sample_id>/download          # ZIP
GET /samples/<sample_id>/download/r1       # R1 gzip
GET /samples/<sample_id>/download/r2       # R2 gzip
GET /samples/<sample_id>/manifest          # manifest JSON
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `DATA_DIR` | `/data` | SQLite、暂存文件和已发布包目录 |
| `MAX_UPLOAD_BYTES` | `17179869184`（16 GiB） | Flask 请求大小上限 |
| `JOB_WORKERS` | `1` | 后台任务线程数；建议保持 1，避免大任务争抢磁盘 |
| `KEEP_RELEASES` | `2` | 每样本保留的已完成 release 数量，最小值按 2 处理 |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `HOST_PORT` | `5000` | Compose 映射到宿主机的端口 |

## 本地无 Docker 运行

需要 Python 3.11+，并先安装依赖：

```bash
python3 -m pip install -r requirements.txt
DATA_DIR=./data python3 app.py
```

生产容器使用单 Gunicorn worker + 多线程，以保证后台 `ThreadPoolExecutor` 只有一个写入方。若直接使用 Gunicorn，也应保持 `--workers=1`。

## 测试

核心逻辑只依赖 Python 标准库，可直接运行：

```bash
PYTHONPATH=. python3 -I -m unittest discover -s tests -t . -v
```

安装 Flask 后，Web 集成测试也会自动执行；当前开发环境未安装 Flask 时，3 个 Flask 集成测试会显示为 skipped，其余 ingest/publish 测试仍完整运行。

## 目录说明

```text
app.py                  # Flask 路由、任务状态、上传与后台执行器
ingest.py               # 流式 gzip/FASTQ 校验、SQLite 配对、同步输出
publish.py              # release、manifest、ZIP、原子 symlink 发布
templates/              # 网页模板
static/                 # CSS 和前端轮询逻辑
tests/                  # unittest 测试
examples/               # 乱序成功、末尾缺配、末尾 gzip 损坏示例
tools/make_examples.py  # 示例生成脚本
Dockerfile
docker-compose.yml
requirements.txt
```
