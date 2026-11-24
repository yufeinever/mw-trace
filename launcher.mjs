import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

const directory = path.dirname(fileURLToPath(import.meta.url));
const url = "http://127.0.0.1:8770/";

async function dashboardReady() {
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(1000) });
    return response.ok && (await response.text()).includes("质量团队对话TRACE追踪工具");
  } catch {
    return false;
  }
}

function openBrowser() {
  const child = spawn("cmd.exe", ["/d", "/s", "/c", `start "" "${url}"`], {
    detached: true,
    stdio: "ignore",
    windowsHide: true,
  });
  child.unref();
}

if (await dashboardReady()) {
  console.log("观察台已经在运行，正在打开浏览器...");
  openBrowser();
  process.exit(0);
}

console.log("正在启动 MobileWork 对话跟踪观察台...");
console.log("运行期间请保留此窗口；关闭窗口即可停止观察台。\n");
const server = spawn(process.execPath, [path.join(directory, "server.mjs")], {
  cwd: directory,
  env: {
    ...process.env,
    MOBILEWORK_SQLITE: path.join(directory, "runtime", "sqlite3.exe"),
  },
  stdio: "inherit",
});

let opened = false;
for (let attempt = 0; attempt < 30 && !opened && server.exitCode == null; attempt += 1) {
  await new Promise(resolve => setTimeout(resolve, 500));
  if (await dashboardReady()) {
    opened = true;
    openBrowser();
  }
}

if (!opened && server.exitCode == null) {
  console.error("启动超时。请确认 MobileWork 已安装，并把此窗口中的提示发给维护人员。");
  server.kill();
  process.exitCode = 1;
} else if (server.exitCode == null) {
  await new Promise(resolve => server.once("exit", resolve));
}

if (server.exitCode && server.exitCode !== 0) {
  console.error("\n观察台启动失败。请确认 MobileWork 已运行，然后把此窗口中的错误发给维护人员。");
  process.exitCode = server.exitCode;
}
