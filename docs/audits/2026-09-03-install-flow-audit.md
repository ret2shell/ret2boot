# 安装流程审计问题清单（2026-09-03）

- **审计对象**：ret2boot 安装器全流程（questionnaire → plan review → 原子步骤 → journal/resume → 维护命令）
- **方法**：5 路独立 subagent 并行审计（主流程编排 / 原子步骤 / 配置与 journal / 本地化资源 / 启动提权更新），交叉核对去重后，关键结论逐一以代码验证。
- **当前状态（2026-09-03 第 1 轮修复完成）**：12 条 P0、8 条 P1、14 条 P2 中的绝大多数及 D2/D6 已修复（见各条目标注的 commit）；仅剩 D1/D3/D4/D5/D7/D8 六条设计层问题待后续决策，详见文末追加记录。
- **状态标记**：`open` = 待处理；`confirmed` = 已核实属实；`fixed` = 已修复。发现新问题追加到文末「追加审计记录」章节，并分配递增 ID。
- **严重度**：P0 高危 / P1 明显不一致 / P2 冗余可收敛 / P3 设计层面。

---

## P0 — 高危（潜在 Bug）

### B1 · `invalidate_install_pipeline` 擦除卸载所需的关键元数据 · fixed (`054d157`)
- 位置：`src/config.rs`（invalidate_install_pipeline / set_install_review_confirmed）、`src/install/mod.rs:139-151`
- 任何问卷答案变化都会清空 `execution.steps`，连同 `release_name`、`ingress_installed_by_ret2boot`、`owned_by_ret2boot`、`gateway_http_port` 等外部系统所有权记录一并丢失；而 `uninstall_existing` 恰恰依赖这些元数据识别已安装组件（仅对 `release_name` 有兜底过滤）。改过一个答案后执行卸载 → helm release / ingress-nginx / 外装 nginx 成为孤儿，不会被卸载。`set_install_review_confirmed(false)` 同样清空 steps。

### B2 · 系统配置缺失时维护命令静默回退到本地 XDG 配置并可执行破坏性卸载 · fixed (`bf78b1b`)
- 位置：`src/main.rs:70-87`、`src/install/mod.rs:130-157`
- `load_system_config` 返回 None（/etc 下无配置）时继续用用户本地配置；若本地配置残留问卷状态（node_role 等），`uninstall_existing` 的 `should_include` 过滤会让 cluster/platform 步骤照常执行 `k3s-killall.sh` / helm uninstall——对一台实际没装过平台、只是跑过一半问卷的机器。

### B3 · 卸载（uninstall）全程零确认提示 · fixed (`bf78b1b`)
- 位置：`src/install/mod.rs:130-157`、`src/startup.rs`（initialize_maintenance）
- install 路径有「安全确认 + 计划复核」两道确认；uninstall 没有任何 ConfirmCollector，直接销毁整个集群和平台。全仓库仅 install.review 一处确认（install/mod.rs:604）。

### B4 · /tmp 临时文件短暂泄露明文密钥 · fixed (`4cd55a3`)
- 位置：`src/startup.rs:44-69`（persist_system_config_copy）
- `fs::write` 以默认 umask 创建含 signing_key、数据库密码、worker token 的临时 TOML（通常 0644），之后才 `install -m 600`；且每次 `persist_change`（即每个问卷答案）都触发一次该流程，高频写在共享 /tmp 留下敏感窗口。写完后 `fs::remove_file` 是普通用户删除，与 privileged install 的属主 root 可能不匹配，文件可能残留。

### B5 · `config.save()` 非原子写 · fixed (`4cd55a3`)
- 位置：`src/config.rs:447-459`
- 直接 `fs::write` 覆盖配置文件。中断/崩溃会损坏唯一的 journal + 问卷状态；resume 机制自身依赖的文件没有崩溃保护（对比：系统副本反而用 install 原子化写入）。

### B6 · ASCII 终端强制 en-us 并永久覆写用户语言偏好 · fixed (`4cd55a3`)
- 位置：`src/startup.rs:90-101`
- 检测到非 UTF-8 终端时 `config.set_language("en-us")` 并持久化——用户的语言选择被永久破坏，下次在 UTF-8 终端也不会恢复。

### B7 · 防火墙守卫端口与集群实际迁移端口不一致 + K3s 端口迁移整体缺失 · fixed (`bb67360`)
- 位置：`src/install/steps/cluster.rs:40,475-483,595-640`、`src/install/steps/gateway.rs:43-44,1320-1330`
- `choose_available_gateway_ports` 从 [10080,11080,...] 动态选端口并持久化，但仅 `install_rke2` 使用；iptables 守卫对 Ingress 模式保护的却是硬编码 `10080/10443`。若 10080 被占用而选了 11080，守卫规则保护错端口。
- K3s 分支完全没有端口迁移逻辑（`install_k3s` 从不调用 `selected_gateway_ports_for_cluster`，也不写 K3S_TRAEFIK_CONFIG_DEST），但 describe() 无差别宣称 "gateway_ports_shifted"（describe/install 脱节），且 `cleanup_k3s_gateway_manifests` 会清理一个从未安装过的文件。

### B8 · 双 ingress-nginx 控制器潜在冲突（RKE2） · fixed (`bb67360`)
- 位置：`src/install/steps/cluster.rs:483`、`src/install/steps/gateway.rs:499-516`
- RKE2 默认自带 rke2-ingress-nginx（cluster 步骤还为其写端口迁移配置），而 ApplicationGatewayStep 又通过 helm 另装一个 `ingress-nginx` release（DaemonSet hostNetwork 占 80/443）。两者并存，端口/路由语义混乱。

### B9 · k3s/rke2 安装脚本无版本锁定，resume 会造成意外升级 · fixed (`c9a45df`)
- 位置：`src/install/steps/cluster.rs:401-440,846-851`
- `get.k3s.io` 不设 `INSTALL_K3S_VERSION`；cluster 步骤没有幂等检查（helm/platform 都有）。install 中断后 resume 重跑 `install_k3s` → 将已有集群升级到 latest。`reconcile_existing`（cluster.rs:46-50）与 install() 完全同体，install 与 update 的边界模糊。RKE2 同理。
- 整套版本策略分裂：helm=latest 未钉、ingress-nginx chart 无 `--version`、platform chart=release 钉死、镜像 tag=常量钉死、k3s/rke2=latest——五种策略互不一致。

### B10 · 卸载后托管 TLS 资产与 /srv/ret2shell 泄露 · fixed (`c9a45df`)
- 位置：`src/install/steps/gateway.rs:596-599`、`src/install/steps/platform.rs`（uninstall）
- 安装时写 `/etc/ret2shell/tls/<name>/fullchain.pem`、`privkey.pem`，但 gateway uninstall（两个分支）和 platform uninstall 均不清理该目录；platform uninstall 也不删除 `/srv/ret2shell` 树——与 nginx 卸载「删干净」的行为不一致。

### B11 · 回滚失败会覆盖原始错误信息 · fixed (`c9a45df`)
- 位置：`src/install/mod.rs`（rollback_failed_installation）
- 回滚失败时再次 `mark_install_step_failed` + `record_failure(Rollback)`，把原始 Install 错误从 `last_failure`（单槽位）和 step.last_error 中覆盖掉。

### B12 · 平台配置应走 Secret 实际却挂载宿主机 hostPath（明文 + 节点锁定） · confirmed
- 位置：`src/install/steps/platform.rs:89-136,54,2225-2281,1922-1943,3299-3306,1966`
- 原理上 ret2shell-config 应是集群内 Secret（chart 也确实渲染并创建了 `Secret ret2shell-config`），但 `patch_platform_chart_template` 把 chart 原模板中挂载该 Secret 的 volumes 块整体替换（PATCHED_PLATFORM_VOLUMES 只剩 `frontend`、`backend-config` 两个 hostPath），`/etc/ret2shell` 实际挂载的是 hostPath `/srv/ret2shell/backend/config`。后果：
  - 配置明文落在宿主机目录，绕过 k8s Secret 机制；
  - Pod 被隐式节点锁定——PV 有 nodeAffinity 钉在 master（platform.rs:3995-4002），但 StatefulSet 本身没有 nodeSelector，frontend/backend-config 的 hostPath 依赖 PVC 绑定的副作用才不会飘到其他节点；若 persistence 关闭或用 existingClaim，Pod 可被调度到无配置文件的节点上静默失败。
- **配套不一致（同一族问题）**：
  - Secret 与宿主机文件内容本身就分叉：Secret 里是 chart 生成的原始 config.toml，宿主机文件是 `merge_runtime_config_with_reference` 合并参考配置后的版本——合并结果只落 host 文件，永不回写 Secret；而 `config_changed`（platform.rs:1966）却把 Secret 内容当比对基线，语义错位；
  - `blocked.txt` 同样双源：chart 渲染 `ConfigMap ret2shell-blocked`（3310-3316），Pod 实际读 host 文件 `BACKEND_BLOCKED_DEST`；
  - `license` 只落 host 文件（`BACKEND_LICENSE_DEST`，mode 644），无任何集群对象；
  - 对照组：TLS 却用了正确的 `kubectl create secret tls` 机制（platform.rs:2047）——同一步骤内三种配置分发机制并存；
  - `workload_changed` 混入 `host_license_changed`（platform.rs:1963）——license 是 host 文件变更，却触发整个 helm upgrade。

---

## P1 — 明显不一致

### I1 · 四条命令路径的编排重复且互不整齐 · fixed (`5007b0f`)
- 位置：`src/main.rs:70-87`、`src/install/mod.rs:75-127,243-303,305-347,690-770`
- main.rs 三个维护分支逐字重复 `initialize_maintenance + load_system_config + config 替换` 块；sync_existing 与 update_existing 90% 相同（仅差 cluster reconcile 与 sync mode）；`run_maintenance_step` vs `execute_installation` 步骤循环两套并行实现（start/run/complete/fail 生命周期）；`run_maintenance_uninstall_step` 又是第三套（不记 attempts、不管 phase）。

### I2 · telemetry::init 时机混乱、4 处调用 · fixed (`5007b0f`)
- 位置：`src/install/mod.rs:79,105,134,647`、`src/startup.rs`（tracing 日志）
- install 路径要到 `prepare_installation`（问卷+复核之后）才初始化 → `run()` 开头的 `info!` 和 startup.rs 里所有 tracing 日志全部丢弃；sync/update 在开头初始化，uninstall 完全不初始化——三条路径三种行为。错误记录同样分裂：仅 install 路径记录 `InstallFailureStage::Startup`（main.rs:55-62），维护命令初始化失败不记录；install 流内 record_failure 会同步系统副本，main.rs:58 的 record_install_failure 不同步。

### I3 · 维护路径硬编码英文，绕过 i18n · fixed (`5007b0f`)
- 位置：`src/install/mod.rs:158,228,230,258,286,295-296,307,327,336-337,347-379`、`src/main.rs:38`、`src/errors.rs:18`
- "Synchronizing installed platform"、"Running:"、"error details:"、整段 sync report——而 locale 里存在等价 key（`install.execution.step_running/step_done/step_failed_detail`）。维护命令输出语言与所选 locale 脱节。

### I4 · `--force` 语义与文档不符 · fixed (`5007b0f`)
- 位置：`src/cli.rs:11-13`、`src/install/steps/platform.rs:1992`
- 帮助文本只说是 "StatefulSet evacuation before Helm upgrade"；实际唯一生效点为 platform.rs:1992（`ctx.force_upgrade() && release_exists`），install/sync/update 都生效，**uninstall 接受并静默忽略**。内部名 `force_upgrade` 与 CLI 名 `force` 不一致。

### I5 · `InstallFailureStage` 语义滥用 · fixed (`5007b0f`)
- 位置：`src/install/mod.rs`（run_maintenance_uninstall_step / run_maintenance_step）、`src/config.rs:166,180`
- uninstall 步骤失败记录为 `Rollback` 阶段（卸载不是回滚）；sync/update 步骤失败记录为 `Install` 阶段；`Completion` 变体定义后从未使用（死代码）；`enter_review_phase` 与 `ensure_install_confirmation` 两个不同动作共用 Review 阶段。

### I6 · journal/phase 字段只写不读，resume 名不副实 · fixed (`57905e5`)
- 位置：`src/config.rs:19,211,1002,1014,493-501`、`src/install/mod.rs`、`src/startup.rs:87`
- `install.execution.phase` 只有写入、无任何读取方——resume 不会按 phase 跳过问卷，每次 resume 重新跑全部问卷提示（与 locale 中 "resume_ready / Continuing from the last recorded step" 的承诺不符）；`attempts` 仅用于变更检测，从未展示；`last_failure` 持久化后从不显示给用户；`terminal.charset` 每次启动写入、全仓库无读取方，还因此触发一次无谓的 config.save。

### I7 · 本地化严重落后 + 死键堆积 · confirmed
- 位置：`src/resources/locales/*.yml`
- **zh-hant 与 ja-jp 各缺 49 个 key**（en-us/zh-hans 286 个 vs 237 个），其中约 35 个是活 key：整套 TLS 问卷（18 个）、nodeport_security（4 个）、entry_prompt/entry_options、disable_traefik、registry_mirrors、ingress_host_network/nodeport_guard 等——rust_i18n fallback 到 en-us，日文/繁中用户在核心 TLS/安全问卷处直接看到英文。
- 死键（代码 0 引用，已 grep 验证）：`install.exposure.prompt`、`install.exposure.options.*`、`install.exposure.profile_locked`、`install.deployment_profile.*`（7 个）、`install.platform.public_host.prompt` 基础版 + `.local_lab/.campus_internal/.public_domain`、`.derived_note`、`install.steps.gateway.selected_profile`、`install.steps.platform_plan.profile/.service_account/.rbac`、`install.preflight.details.kernel_loadable`——旧 questionnaire 流（deployment_profile / nip.io 时代）的遗留，且只存在于 en-us/zh-hans，加剧 4 语言不对称。
- 术语漂移：zh-hans "RKE2 安装源连通性"（大写）vs 其余三语 "rke2"；ja-jp クラスター/クラスタ 混用；同一 nginx 在问卷叫 "外置/external nginx"、在计划细节叫 "系统/system nginx"；同一概念两代文案共存（exposure.prompt vs entry_prompt）。

### I8 · 死资源：整个 templates/ 目录 + resources.rs 加载器 · confirmed
- 位置：`src/resources.rs`（load_utf8，标记 `#[allow(dead_code)]`，全仓库零调用）、`src/resources/templates/`（4 个文件无任何代码引用）
- nginx 模板与 gateway.rs `render_nginx_site`（:787-870）生成的配置重复且已漂移（模板缺 /assets/ location、root、TLS 块）；stream 模板（ssl_preread 443 → 后端 HTTPS）与现行实现（nginx 侧 TLS 终结）矛盾；k8s 模板 StorageClass 用 `rancher.io/local-path`，代码生成的用 `kubernetes.io/no-provisioner`（platform.rs:3954-3959）——两套冲突的「真相」，模板里还有重复的 `pods` 资源项；install.env.example 引用 `RET2SHELL_CHANNEL/COMPONENTS=agent,caddy/ENABLE_SYSTEMD/NODE_NAME`，安装器代码完全不读这些变量（仅 `RET2SHELL_LOG`），属另一产品线的陈旧残留。

---

## P2 — 冗余（应收敛为共享实现）

### R1 · `is_root_user` 三份逐字拷贝 · fixed (`3cfce0b`)
- `src/privilege.rs:62`、`src/update.rs:729`、`src/config.rs:1192`。应收敛为单一来源。

### R2 · sha256sum 解析器两份分叉 · fixed (`3cfce0b`)
- `src/update.rs:653-686` vs `src/install/steps/helm.rs:364-394`（helm 连 `rsplit(['/','\\'])` 都内联复制，且两版行为有差异）。应提取共享 checksum 解析模块。

### R3 · 版本解析两份 · fixed (`3cfce0b`)
- `src/update.rs:690-696` `parse_release_version` vs `src/install/steps/helm.rs:348-361` `parse_helm_version_manifest`。

### R4 · User-Agent 四个调用点三种写法 · fixed (`3cfce0b`)
- `src/update.rs:698-700`、`src/install/steps/helm.rs:421-423`、`src/install/steps/preflight.rs:620,632`（内联）。

### R5 · 临时路径生成两份（且都只用裸纳秒时间戳，无 pid/随机，并发有碰撞风险） · fixed (`3cfce0b`)
- `src/startup.rs:45-50` vs `src/install/steps/support.rs:53-59` `unique_temp_path`。

### R6 · "staged → privileged install -D -m" 模式多种变体 · fixed (`3cfce0b`/`4cd55a3`)
- `src/startup.rs:53-63`（600）、`src/install/steps/support.rs:99-110`（600）、`src/install/steps/helm.rs:230-240`（755），加上 `src/install/steps/gateway.rs` 内联 `install -m 600`（无 -D）和 platform.rs `sync_managed_file`/`sync_managed_text_file` 两个近重复 helper——同一操作 5 种变体。

### R7 · `GitHubAuth::from_gh` 重复执行 · fixed (`3cfce0b`)
- `src/update.rs:123,258`：UpdateManager 与 Ret2ShellChartManager 各 spawn 一次 `gh auth token`，每次启动至少两个子进程 + 重复输出同一 warn 日志。

### R8 · 跨步骤常量/函数重复定义（漂移风险） · fixed (`3cfce0b`)
- `INTERNAL_REGISTRY_NODE_PORT=30310`：gateway.rs:45 与 platform.rs:75；
- `K3S_REGISTRIES_DEST`/`RKE2_REGISTRIES_DEST`：cluster.rs:28 与 platform.rs:70；
- `application_exposure_label`：cluster.rs:888 与 gateway.rs:945 完全相同；
- 端口 6443/9345/2379/2380/10250 在 gateway.rs 常量化，preflight.rs 却用内联字面量。

### R9 · 集群访问抽象三重复制 · fixed (`3cfce0b`)
- `ClusterAccess`（platform.rs:4267）、`GatewayClusterAccess`（gateway.rs:94）、WorkerPlatformProbeStep 内联 kubectl 探测（第三份）；连 `wait_for_nodes_ready` 都复制两份且参数不同（NODE_READY_WAIT_RETRIES=60 vs CLUSTER_READY_WAIT_RETRIES=120，同为 5s 间隔，超时总额差一倍，无理由的不一致）。

### R10 · collectors 标注过期 / 真死代码 · fixed (`3cfce0b`)
- `src/install/collectors/input.rs:8,14` `#[allow(dead_code)]` 标注过期（InputCollector 被大量使用）；`MultiSelectCollector` + `mod.rs:9-11` 再导出全仓库零使用，是真死代码。

### R11 · 问卷每答一题双写盘 · fixed (`3cfce0b`)
- 每个答案触发 `config.save()` + `persist_system_config_copy()`；以 root 运行时两者写同一个文件、两种机制（fs::write vs install）——纯冗余；`persist_system_config_copy` 与 `Ret2BootConfig::save` 的 TOML 序列化逻辑也重复。

### R12 · 死代码 / 死配置字段 · fixed (`3cfce0b`)
- `PlatformTlsConfig.domains/acme_email/acme_dns_provider/acme_dns_credentials`：无写入无读取（仅 serde + locale 提示语残留）；
- `PlatformTlsMode::AcmeDns` 是 `#[default]` 却被 collect/normalize 强制归一为 ProvidedFiles——默认值指向一个被流程明确废弃的变体；`collect_platform_tls` 里 `AcmeDns | ProvidedFiles =>` 分支实际不可达；
- `install.env.example` 同 I8；
- `collect_nodeport_security` 的 `_exposure` 占位绑定 + guard_enabled=true 分支空操作；
- `storage_host_subdirectory`（platform.rs）恒返回 None 的无用 match。

### R13 · 不对称的幂等与重试策略 · fixed (`eb91405`)
- helm 下载 3 次重试 60s，`stage_remote_script` 单次尝试 30s（同一「下载远程内容」行为两套标准，且 root 执行的 k3s/rke2 脚本反而无 checksum 校验，helm 却有）；
- registry 配置：cluster 步骤无条件覆盖写入（且初始写入不带 internal registry host，platform 同步时再带 host 重写一遍 + 重启 runtime——同一安装里 registries.yaml 被写两次、内容不一致）。

### R14 · 卸载/回滚严格性分裂 · fixed (`eb91405`)
- cluster `uninstall_*` 传播脚本错误、`rollback_*` 吞错；uninstall 前置 `from_plan_context` 要求完整问卷 spec（application_exposure 缺失即 bail）——卸载不应依赖问卷完整性；gateway uninstall 对缺失 mode metadata 默认按 Ingress 处理（可能跳过外装 nginx 清理）。

---

## P3 — 设计层面

### D1 · `ret2boot install` 重复运行是 no-op · open
- 全部完成后 review.confirmed 持久 → 跳确认 → 全部 Completed 跳过。需确认是否是有意的 resume 语义（用户预期可能是重装）。

### D2 · maintenance 每步成功都 `set_install_phase(Completed)` · fixed (`d0f6f2c`)
- 与 install 流「结束时一次性置位」语义不一致，maintenance 在持续覆写 install journal。

### D3 · `persist_change` 的 (field, value) 调试参数用途混乱 · open
- 失败路径把 error_text 当 value 传（install/mod.rs 289-297 及 execute 错误路径），成功路径传 step id。

### D4 · 更新下载是死端 · open
- `src/update.rs`：`UpdateCheckResult::Downloaded` 只打印路径，无任何自更新执行逻辑；且网络检查/下载在提权之前、对维护命令也执行。

### D5 · `load_system_config` 实现冗余 + 双源真相 · open
- `src/main.rs:93-104`：`test -f` + `cat` 两次特权往返 + 独立 TOML 解析（与 `Ret2BootConfig::load` 重复）；维护命令把系统配置整包替换进内存后，后续 save 又写回用户路径（非 root 时）。

### D6 · `plan_step_title` 只返回 step id 字符串 · fixed (`3e03d0f`)
- 回滚消息因此显示 kebab-case id 而非人类可读标题（execute 路径有真标题可用）。

### D7 · 平台 init manifest 在 helm 之外 apply · open
- Namespace×2、ClusterRoleBinding（绑定 cluster-admin）由 kubectl apply 管理，卸载依赖文件存在才清理；cluster-admin 级 RBAC 的必要性存疑。

### D8 · registries.yaml 初始写入内容不完整 · open
- cluster 步骤 `sync_cluster_registry_config` 传 `internal_registry_host=None`，platform 步骤同步时带 host 重写——若安装中断在两步之间，集群内 registry 镜像配置处于中间态。

---

## 已核实无问题的部分

- `src/checksum.rs`：单一干净的 sha256 实现，update.rs / helm.rs 均复用。
- `src/privilege.rs:284-286`：`shell_quote` 单引号转义正确，su 后端无注入缺口；sudo keepalive 停止通道 + Drop join 无泄漏死锁。
- `src/update.rs:653-685`：`parse_sha256sum_line` 有完整测试覆盖（含 Windows 反斜杠、二进制 `*` 前缀）。
- `src/install/collectors/*`：dialoguer + ColorfulTheme、错误处理风格统一。
- `src/install/steps/gateway.rs:37`：iptables insert 位置参数（`-I PREROUTING 1`）语法正确；purge 循环以 `-D` 失败终止，无死循环。
- 问卷答案即时持久化、stored answer 作为默认值——各步骤 collect 实现一致。
- registry 顺序（preflight → cluster → helm → gateway → platform → worker-probe）与步骤间依赖（TLS 资产在 platform 前、helm 在 gateway 前）一致。

---

## 追加审计记录

> 后续审计在此追加，格式：`### B/I/R/D XX · 标题 · 状态`，并同步更新上方清单状态。

