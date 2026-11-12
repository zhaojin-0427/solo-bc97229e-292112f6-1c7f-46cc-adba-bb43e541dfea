# FASTQ 示例

所有目录中的 `R1.fastq.gz` / `R2.fastq.gz` 都可直接在网页表单上传。
`preview.txt` 只用于人工查看，不需要上传。

| 目录 | 样本号示例 | 上传后预期 |
|---|---|---|
| `valid-disordered/` | `DEMO-ORDERED` | 成功。输入 R1 顺序是 `DEMO-03, DEMO-01, DEMO-04, DEMO-02`，R2 输入顺序完全不同；发布的 R1、R2 都按 R1 原顺序同步排列。 |
| `tail-missing-pair/` | `DEMO-TAIL-MISSING` | 失败。R1 有 3 条，R2 只有前 2 条；扫到 R2 末尾才发现 `TAIL-03` 缺配，不发布任何结果。 |
| `tail-corrupt-gzip/` | `DEMO-TAIL-CRC` | 失败。R2 的三条记录都能读出，但 gzip 末尾 CRC 被人为改坏；读到末尾才发现校验错误，不发布任何结果。 |

## 手工验证

```bash
# 有效示例：解压后查看顺序是否已经被重排
python3 - <<'PY'
import gzip
for side in ("R1", "R2"):
    with gzip.open(f"examples/valid-disordered/{side}.fastq.gz", "rb") as fh:
        print(side, [line.decode().split()[0] for line in fh if line.startswith(b"@")])
PY
```

重新生成示例（内容确定，gzip mtime 固定为 0）：

```bash
python3 tools/make_examples.py
```

注意：`tail-corrupt-gzip/R2.fastq.gz` 是故意损坏的文件；普通 `gzip -t` 也会报 CRC 错误。
