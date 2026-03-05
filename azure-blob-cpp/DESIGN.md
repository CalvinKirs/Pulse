# Azure Blob Connectivity Client 设计文档（可重建实现）

## 1. 目标

本文档用于在其他代码仓库或调试环境中，重建一个与当前 `azure_blob_connectivity` 基本一致的 C++ 命令行工具。

“一致”定义：

- 命令行参数、默认值、校验规则一致
- 主流程和核心函数划分一致
- 成功/失败输出格式和退出码一致
- Azure SDK 使用方式一致（共享密钥认证 + BlobContainerClient）

## 2. 产物与目录结构

最小工程结构：

```text
azure-blob-connectivity-client/
  CMakeLists.txt
  vcpkg.json
  src/
    main.cpp
  build_macos.sh
  build_linux.sh
```

可选：

- `README.md`
- 打包脚本（如 `package_macos.sh`）

## 3. 构建约束

### 3.1 CMake 约束

- `cmake_minimum_required(VERSION 3.16)`
- `project(... LANGUAGES CXX)`
- `CMAKE_CXX_STANDARD=17`
- `CMAKE_CXX_STANDARD_REQUIRED=ON`
- `CMAKE_CXX_EXTENSIONS=OFF`
- `find_package(azure-storage-blobs-cpp CONFIG REQUIRED)`
- 目标名：`azure_blob_connectivity`
- 链接：`Azure::azure-storage-blobs`

### 3.2 依赖约束

`vcpkg.json` 最小依赖：

- `azure-storage-blobs-cpp`

当前参考版本（允许同大版本替换）：

- azure-core-cpp `1.16.x`
- azure-storage-common-cpp `12.12.x`
- azure-storage-blobs-cpp `12.16.x`

## 4. 运行时 CLI 规格（必须一致）

### 4.1 参数列表

| 参数 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `--account-name` | 是 | 无 | Azure Storage account name |
| `--account-key` | 是 | 无 | Azure Storage account key |
| `--container` | 条件必填 | 无 | Container 名。若 `--container-url` 为空则必填 |
| `--endpoint` | 否 | `blob.core.windows.net` | endpoint 主机名，允许带 `http://` / `https://` |
| `--container-url` | 否 | 空 | 完整容器 URL，例 `https://acct.blob.core.windows.net/container` |
| `--mode` | 否 | `basic` | `basic` 或 `rw` |
| `--prefix` | 否 | `doris-connectivity-check/` | `rw` 模式临时 blob 前缀 |
| `--log-level` | 否 | `3` | Azure SDK Logger level，整数 0~4 |
| `--help`, `-h` | 否 | 无 | 打印 usage 并退出 |

### 4.2 参数语法

必须支持两种写法：

- `--key value`
- `--key=value`

### 4.3 参数解析细节（按当前实现）

1. 非 `--` 开头参数视为错误：`Unexpected argument: <arg>`。
2. `--key` 后如果缺值，或下一个 token 仍是 `--...`，报错：`Missing value for --<key>`。
3. `--mode` 在校验前先转小写。
4. `--log-level` 用 `std::stoi`，异常时报：`Invalid --log-level: <raw>`。
5. 只校验“必需字段和取值范围”；未知 flag 不报错（被忽略）。

## 5. 退出码与输出规范（必须一致）

### 5.1 退出码

- `0`：成功
- `1`：运行失败（网络、认证、Azure 请求异常、运行时异常）
- `2`：参数错误

### 5.2 参数错误输出

格式：

```text
Argument error: <err>

<usage全文>
```

随后返回 `2`。

### 5.3 help 输出

传入 `--help` 或 `-h` 时打印 usage，返回 `0`。

### 5.4 成功输出

`basic` 成功输出：

```text
[PASS] container reachable
  url: <container_url>
  etag: <etag>
```

`rw` 成功输出：

```text
[PASS] read/write/delete succeeded
  container: <container_url>
  blob: <generated_blob_name>
```

### 5.5 Azure 请求失败输出

捕获 `Azure::Core::RequestFailedException` 时，输出必须包含以下字段并返回 `1`：

```text
[FAIL] Azure request failed
  what: <ex.what()>
  message: <ex.Message>
  status_code: <int(ex.StatusCode)>
  error_code: <ex.ErrorCode>
  request_id: <ex.RequestId>
```

### 5.6 其他异常输出

```text
[FAIL] Unexpected error: <ex.what()>
```

返回 `1`。

## 6. 核心算法规格

### 6.1 数据结构

```cpp
struct Options {
  std::string account_name;
  std::string account_key;
  std::string container;
  std::string endpoint{"blob.core.windows.net"};
  std::string container_url;
  std::string mode{"basic"};
  std::string prefix{"doris-connectivity-check/"};
  int log_level{3};
};
```

### 6.2 URL 生成逻辑

`make_container_url(opts)`：

1. 若 `opts.container_url` 非空：返回去掉结尾 `/` 后的值。
2. 否则：
   - `endpoint = normalize_endpoint(opts.endpoint)`
   - `normalize_endpoint` 规则：
     - 去掉尾部连续 `/`
     - 去掉前缀 `https://` 或 `http://`
   - 返回：`https://{account_name}.{endpoint}/{container}`

### 6.3 prefix 和临时对象名

- `normalize_prefix`：如果非空且不以 `/` 结尾，则补 `/`。
- 临时 blob 名：
  - `ms = system_clock::now()` 的 epoch milliseconds
  - `blob_name = normalize_prefix(prefix) + "azure-connectivity-" + std::to_string(ms)`

### 6.4 客户端初始化

按以下顺序：

1. `Logger::SetLevel(static_cast<Logger::Level>(opts.log_level))`
2. `credential = std::make_shared<StorageSharedKeyCredential>(account_name, account_key)`
3. 创建 `BlobClientOptions client_options`
4. `client_options.Retry.StatusCodes.insert(HttpStatusCode::TooManyRequests)`
5. 用 `container_url + credential + client_options` 创建 `BlobContainerClient`

## 7. 模式行为

### 7.1 basic 模式

执行：`container_client.GetProperties()`。

- 无异常：按成功格式输出，返回 `0`
- 有异常：走统一异常处理

### 7.2 rw 模式

步骤必须一致：

1. 生成 `blob_name`
2. `payload = "azure-connectivity-payload:" + blob_name`
3. `blob_client = container_client.GetBlockBlobClient(blob_name)`
4. `UploadFrom(reinterpret_cast<const uint8_t*>(payload.data()), payload.size())`
5. `GetProperties()` 并校验 `BlobSize == payload.size()`
6. `Delete()`
7. 输出成功信息，返回 `0`

异常回收策略：

- 维护 `uploaded` 布尔值
- 若上传后任意步骤抛异常，尝试 `Delete()`，删除失败吞掉异常
- 然后重新抛出，交给 `main` 统一输出失败

特殊失败分支：

- 若 `BlobSize` 不匹配：
  - 输出
    - `[FAIL] uploaded blob size mismatch, expected=<exp>, actual=<act>`
  - 尝试删除 blob（失败忽略）
  - 返回 `1`（不抛异常）

## 8. 函数切分建议（用于重建一致代码）

建议保留以下函数名与职责：

- `to_lower(std::string)`
- `starts_with(const std::string&, const std::string&)`
- `trim_trailing_slash(std::string)`
- `normalize_endpoint(std::string)`
- `normalize_prefix(std::string)`
- `make_container_url(const Options&)`
- `make_temp_blob_name(const std::string&)`
- `print_usage(const char*)`
- `parse_args(int, char**, Options*, std::string*)`
- `run_basic(const BlobContainerClient&, const std::string&)`
- `run_rw(const BlobContainerClient&, const std::string&, const std::string&)`
- `main(int, char**)`

## 9. 主流程伪代码

```text
main:
  opts, err
  if !parse_args:
    print "Argument error: ..." + usage
    return 2

  if argv contains --help or -h:
    print usage
    return 0

  try:
    set azure logger level
    container_url = make_container_url(opts)
    credential = StorageSharedKeyCredential(opts.account_name, opts.account_key)
    client_options.Retry.StatusCodes += 429
    container_client = BlobContainerClient(container_url, credential, client_options)

    if opts.mode == "rw":
      return run_rw(...)
    else:
      return run_basic(...)

  catch RequestFailedException ex:
    print [FAIL] Azure request failed + fields
    return 1

  catch std::exception ex:
    print [FAIL] Unexpected error
    return 1
```

## 10. 构建脚本行为基线

### 10.1 build_macos.sh

- 仅在 `Darwin` 运行
- `INSTALL_DEPS=1` 时调用 `brew install cmake ninja git curl zip unzip tar pkg-config`
- 默认：
  - `VCPKG_ROOT=$HOME/vcpkg`
  - `BUILD_TYPE=Release`
  - `GENERATOR=Ninja`
  - `JOBS=$(sysctl -n hw.ncpu)`
- 若不存在 vcpkg 则 clone
- 若不存在 `vcpkg` 可执行则 bootstrap
- `CLEAN=1` 清空 build 目录
- 通过 toolchain 配置并构建
- 最后检查二进制存在并输出路径

### 10.2 build_linux.sh

- 仅在 `Linux` 运行
- `INSTALL_DEPS=1` 且存在 `apt-get` 时安装：
  - `build-essential cmake ninja-build git curl zip unzip tar pkg-config`
- root 或 sudo 逻辑：
  - root 直接 apt
  - 非 root 且有 sudo：sudo apt
  - 否则报错退出
- 默认：
  - `JOBS=$(nproc)`
  - 其他与 macOS 脚本一致

## 11. 兼容性与平台差异

### 11.1 已验证平台

- macOS arm64（Apple Silicon）可构建运行
- Linux x86_64 设计上可行（依赖系统包 + vcpkg）

### 11.2 稳定性注意事项

1. `--container-url` 与 `--container` 同时给定时，实际使用 `--container-url`。
2. 代码不拒绝未知参数；如果要更严格，可加白名单，但会改变行为。
3. `rw` 模式会写入并删除临时 blob，要求账号有写权限。
4. logger 是全局设置；若嵌入到更大系统要注意全局影响。

## 12. 最小回归测试清单（重写后必跑）

1. `--help` 返回码为 `0`，输出 usage。
2. 缺少 `--account-name` 返回 `2`，错误信息一致。
3. `--mode=BAD` 返回 `2`，报 `Invalid --mode`。
4. 错误账号或 key 触发 Azure `RequestFailedException`，返回 `1` 且打印状态码。
5. `basic` 模式在正确配置下返回 `0` 并输出 `[PASS] container reachable`。
6. `rw` 模式在正确配置下返回 `0` 并输出 `[PASS] read/write/delete succeeded`。
7. `rw` 模式异常时会尝试清理临时对象（可通过 mock 验证）。

## 13. 可选增强（不影响当前一致性）

这些增强不在“当前一致实现”内，若启用请在版本说明中标注：

- 严格拒绝未知 CLI 参数
- 增加 `--timeout-ms` / `--retry-max` 等参数
- 增加 `HEAD` 或下载内容比对
- 增加 JSON 输出模式用于自动化系统集成

## 14. 版本基线

本文档对应的代码基线：

- 工程路径：`/tmp/azure-blob-connectivity-client`
- 主程序：`src/main.cpp`
- 脚本：`build_macos.sh`, `build_linux.sh`

如后续修改，请同步更新本设计文档的“参数、行为、输出、退出码”四类内容。
