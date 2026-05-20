# 极简全能持续运行Agent

## 概述

这是一个**持续运行、自我优化、经验积累**的智能Agent系统。核心设计理念：

```
一直干活 → 越干越会干 → 把学会的全部存起来 → 下次直接调取使用 → 无限循环
```

## 核心特性

| 特性 | 说明 |
|------|------|
| **持续运行** | 开机即运行，崩溃自动重启，永不停止 |
| **三大并行** | 执行任务 + 优化方法 + 存储经验 同时进行 |
| **知识积累** | 经验自动存入知识图谱，越运行越智能 |
| **自我进化** | 从成功/失败中学习，持续改进方法 |

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                    PersistentAgent 主控制器                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   执行引擎    │  │    优化器    │  │   知识图谱   │      │
│  │  (Executor)  │  │ (Optimizer)  │  │ (Knowledge)  │      │
│  │              │  │              │  │              │      │
│  │ - 任务执行   │  │ - 方法改进   │  │ - 经验存储   │      │
│  │ - 代码分析   │  │ - 模式提取   │  │ - 快速检索   │      │
│  │ - Bug修复    │  │ - 策略更新   │  │ - 关联分析   │      │
│  │ - 重构优化   │  │ - 性能监控   │  │ - 标签索引   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │                    保活模块 (Watchdog)                │  │
│  │  - 心跳监控  - 崩溃重启  - 健康检查  - 文件监控      │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 目录结构

```
persistent_agent/
├── main.py              # 主控制器入口
├── run.py               # 启动脚本
├── config.py            # 配置文件
├── core/
│   ├── __init__.py
│   ├── knowledge_graph.py  # 知识图谱模块
│   ├── executor.py         # 执行引擎
│   ├── optimizer.py        # 优化器
│   └── watchdog.py         # 保活模块
├── knowledge_base/      # 知识库存储目录
│   ├── knowledge.db     # SQLite数据库
│   ├── experiences/     # 经验文件
│   └── patterns/        # 模式文件
└── logs/                # 日志目录
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动Agent

```bash
# 基本启动
python run.py --work-dir /path/to/your/project

# 禁用文件监控
python run.py --work-dir /path/to/your/project --no-watch

# 查看状态
python run.py --status

# 导出知识库
python run.py --export
```

### 3. 使用API

```python
from main import PersistentAgent

# 创建Agent
agent = PersistentAgent(work_dir="/path/to/project")

# 启动
agent.start(watch_files=True)

# 提交任务
agent.analyze_code(["src/main.py"], "分析主程序")
agent.fix_bug("src/utils.py", "修复空指针异常")
agent.refactor_code(["src/legacy.py"], "简化复杂逻辑")
agent.implement_feature("添加用户认证功能")

# 获取状态
status = agent.get_status()
print(status)

# 获取改进建议
suggestions = agent.get_improvement_suggestions()

# 导出知识库
agent.export_knowledge("knowledge_backup.json")
```

## 任务类型

| 类型 | 说明 | 示例 |
|------|------|------|
| `analyze` | 代码分析 | 分析代码结构、检测问题 |
| `bug_fix` | Bug修复 | 检测并修复代码缺陷 |
| `refactor` | 代码重构 | 优化代码结构、提升可读性 |
| `implement` | 功能实现 | 实现新功能需求 |
| `optimize` | 性能优化 | 提升代码性能 |
| `test` | 测试执行 | 运行测试用例 |

## 配置说明

编辑 `config.py` 进行自定义配置：

```python
# 执行配置
MAX_WORKERS = 3           # 最大并行任务数
TASK_TIMEOUT = 300        # 任务超时时间(秒)
RETRY_COUNT = 3           # 重试次数

# 保活配置
HEARTBEAT_INTERVAL = 30   # 心跳间隔(秒)
MAX_CRASH_COUNT = 5       # 最大崩溃次数

# 优化配置
OPTIMIZATION_INTERVAL = 60  # 优化检查间隔(秒)

# 监控配置
WATCH_PATTERNS = ["*.py", "*.js", "*.ts"]  # 监控的文件类型
IGNORE_PATTERNS = ["__pycache__", "node_modules", ".git"]  # 忽略的目录
```

## 运行流程

```
1. 启动 → 加载知识图谱 → 恢复历史经验
        ↓
2. 文件监控 → 检测变化 → 自动创建任务
        ↓
3. 任务执行 → 查找相关经验 → 执行 → 记录结果
        ↓
4. 经验存储 → 提取教训 → 存入知识图谱
        ↓
5. 自动优化 → 分析成功率 → 更新策略 → 提取模式
        ↓
6. 循环继续 → 使用优化后的方法 → 效率提升
```

## 知识图谱

### 经验结构

```json
{
  "id": "exp_abc123",
  "task_type": "bug_fix",
  "problem": "空指针异常",
  "solution": "添加空值检查",
  "outcome": "success",
  "lessons": "使用 is None 而非 == None",
  "tags": ["python", "null-check"],
  "success_rate": 0.95,
  "usage_count": 10
}
```

### 模式结构

```json
{
  "id": "pattern_001",
  "pattern_type": "best_practice",
  "name": "空值检查最佳实践",
  "description": "使用 is None 检查空值",
  "effectiveness": 0.9
}
```

## 命令行参数

| 参数 | 说明 |
|------|------|
| `--work-dir, -w` | 指定工作目录 |
| `--no-watch` | 禁用文件监控 |
| `--status` | 显示状态后退出 |
| `--export` | 导出知识库后退出 |

## 日志

日志文件位于 `logs/` 目录：

- `agent_YYYYMMDD.log` - 运行日志
- `agent_state.json` - 当前状态快照

## 注意事项

1. **首次运行**会自动创建知识库和日志目录
2. **崩溃恢复**：崩溃后会自动重启，超过阈值进入安全模式
3. **知识积累**：运行时间越长，经验越丰富，效率越高
4. **资源监控**：自动监控内存和CPU使用，异常时报警

## 扩展开发

### 添加新任务类型

```python
# 在 executor.py 中添加
class TaskType(Enum):
    # 现有类型...
    CUSTOM = "custom"  # 新类型

# 添加处理器
def _handle_custom(self, task: Task) -> ExecutionResult:
    # 自定义处理逻辑
    pass

# 注册处理器
self.handlers[TaskType.CUSTOM] = self._handle_custom
```

### 添加新的优化策略

```python
# 在 optimizer.py 中
strategy = Strategy(
    name="custom_strategy",
    task_type="analyze",
    priority=1,
    conditions=[{"file_count": {"<": 20}}],
    actions=[{"depth": "full"}],
    success_rate=0.9
)
self.strategies["analyze"].append(strategy)
```

## 许可证

MIT License
