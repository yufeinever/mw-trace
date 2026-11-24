# MobileWork 对话 Trace 观察台

用于在本机查看 MobileWork 对话的执行轨迹、模型调用、工具调用及耗时。

## Windows 使用

1. 从 [Releases 页面](https://github.com/yufeinever/mw-trace/releases/latest)下载 `mw-trace-v0.0.35-windows-x64.zip` 并解压。
2. 在同一台 Windows x64 电脑上启动并使用 MobileWork。
3. 双击解压目录中的 `start-dashboard.cmd`。浏览器会打开 `http://127.0.0.1:8770/`。
4. 使用期间保留启动窗口；关闭窗口即可停止观察台。

便携包包含 Node.js 和 SQLite 命令行工具，不需要安装 Node.js 或浏览器插件，也不需要开启 CDP。默认从当前用户的 `.mobilework` 目录读取本机数据；工具只监听本机地址，数据库以只读方式访问。Windows 便携版暂不提供资源监控。

## 开发者

仓库不包含 `runtime/` 中的可执行文件。开发者可以下载便携包获取运行环境，或自行准备 Node.js 22 和 SQLite 命令行工具，并通过 `MOBILEWORK_SQLITE` 指定 SQLite 的完整路径：

```powershell
$env:MOBILEWORK_SQLITE = 'C:\path\to\sqlite3.exe'
node server.mjs
```

可选环境变量 `MOBILEWORK_HOME` 和 `MOBILEWORK_OPENCODE_DB` 用于覆盖默认数据路径。运行 `node --test tests/v0.0.29-evidence.test.mjs` 可执行独立的合成数据检查；准备 `runtime/` 后还可运行 `node --test tests/portable-smoke.test.mjs` 检查 Windows 便携环境。

项目不包含用户实际对话或凭据。运行时产生的数据保留在本机；分享导出文件前，请确认接收方有权查看其内容。第三方运行组件信息见 `THIRD-PARTY-NOTICES.txt`。
