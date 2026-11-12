(function () {
  "use strict";

  function el(id) {
    return document.getElementById(id);
  }

  function setText(id, text) {
    const node = el(id);
    if (node) node.textContent = text;
  }

  function setHidden(id, hidden) {
    const node = el(id);
    if (node) node.hidden = hidden;
  }

  function formatInt(value) {
    const number = Number(value || 0);
    try {
      return number.toLocaleString("zh-CN");
    } catch (_) {
      return String(number);
    }
  }

  function showPanel() {
    const panel = el("job-panel");
    if (panel) panel.hidden = false;
  }

  function setBar(percent) {
    const safe = Math.max(0, Math.min(100, Math.round(Number(percent) || 0)));
    setText("job-percent", safe + "%");
    const bar = el("job-progress-bar");
    if (bar) bar.style.width = safe + "%";
  }

  function renderUploading() {
    showPanel();
    setText("job-status-label", "上传中");
    setText("job-stage", "正在上传 R1/R2 文件…");
    setText("job-r1-reads", "0");
    setText("job-r2-reads", "0");
    setHidden("job-error", true);
    setHidden("job-complete", true);
    setBar(0);
  }

  function renderUploadProgress(percent) {
    setBar(percent);
    setText("job-stage", "正在上传文件… " + percent + "%");
  }

  function setUploadDone() {
    setText("job-status-label", "排队中");
    setText("job-stage", "上传完成，等待后台处理…");
    setBar(100);
  }

  function showError(message) {
    showPanel();
    setText("job-status-label", "失败");
    const error = el("job-error");
    if (error) {
      error.textContent = message;
      error.hidden = false;
    }
  }

  function isTerminal(status) {
    return status === "succeeded" || status === "failed" ||
      status === "interrupted";
  }

  function renderCurrentPackage(job) {
    const box = el("current-package");
    if (!box) return;
    const pkg = job.current_package;
    if (!pkg) {
      setHidden("current-package", true);
      return;
    }
    box.hidden = false;
    let title = "当前可下载版本";
    if (job.status === "running" || job.status === "queued") {
      title = "旧版本仍可下载（新版本尚未发布）";
    } else if (job.status === "failed") {
      title = "本次失败未改动旧包，旧版本仍可下载";
    } else if (job.status === "interrupted") {
      title = "任务中断未改动旧包，旧版本仍可下载，可重新上传";
    } else if (job.status === "succeeded") {
      title = "已发布版本可下载";
    }
    setText("current-package-title", title);
    setText(
      "current-package-meta",
      pkg.read_count + " 对 · release " + pkg.release_id + " · " + (pkg.created_at || "")
    );
    const zip = el("download-package-link");
    const r1 = el("download-r1-link");
    const r2 = el("download-r2-link");
    if (zip) zip.href = pkg.download_url;
    if (r1) r1.href = pkg.r1_url;
    if (r2) r2.href = pkg.r2_url;
  }

  function renderJob(job) {
    showPanel();
    setText("job-sample-label", "样本 " + job.sample_id + " · " + (job.status_label || job.status));
    setText("job-status-label", job.status_label || job.status);
    setText("job-stage", job.stage || "");
    setText("job-r1-reads", formatInt(job.r1_reads));
    setText("job-r2-reads", formatInt(job.r2_reads));
    setBar(job.progress);

    if (job.error) {
      const error = el("job-error");
      if (error) {
        error.textContent = job.error;
        error.hidden = false;
      }
    } else {
      setHidden("job-error", true);
    }
    setHidden("job-complete", job.status !== "succeeded");
    const interruptedBox = el("job-interrupted");
    if (interruptedBox) {
      interruptedBox.hidden = job.status !== "interrupted";
    }
    renderCurrentPackage(job);
  }

  function pollJob(statusUrl, options) {
    const settings = options || {};
    let failures = 0;

    async function tick() {
      try {
        const response = await fetch(statusUrl, {
          headers: { "Accept": "application/json" },
          cache: "no-store"
        });
        if (!response.ok) {
          throw new Error("HTTP " + response.status);
        }
        const job = await response.json();
        failures = 0;
        if (settings.onJob) settings.onJob(job);
        if (job.status === "queued" || job.status === "running") {
          window.setTimeout(tick, 900);
        } else if (settings.onTerminal) {
          settings.onTerminal(job);
        }      } catch (error) {
        failures += 1;
        if (failures <= 4) {
          window.setTimeout(tick, 1500);
        } else {
          showError("无法获取任务进度：" + error.message);
        }
      }
    }

    tick();
  }

  window.IngestUI = {
    renderUploading: renderUploading,
    renderUploadProgress: renderUploadProgress,
    setUploadDone: setUploadDone,
    showError: showError,
    renderJob: renderJob,
    pollJob: pollJob,
    isTerminal: isTerminal
  };
})();
