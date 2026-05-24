"""
极简全能持续运行Agent - 配置文件
"""
import os
from pathlib import Path

# 基础路径
# [Bug C-007] 在某些打包环境或交互式环境中`__file__`不可用时的降级处理
BASE_DIR = Path(__file__).parent.absolute() if '__file__' in dir() else Path.cwd()
KNOWLEDGE_BASE_DIR = BASE_DIR / "knowledge_base"
LOG_DIR = BASE_DIR / "logs"

# 确保目录存在
# [Bug#129] 用try/except包裹目录创建，防止在不可写环境中导入失败
try:
    # 检查路径是否是已存在的文件而非目录
    if KNOWLEDGE_BASE_DIR.exists() and not KNOWLEDGE_BASE_DIR.is_dir():
        raise NotADirectoryError(f"路径已存在但不是目录: {KNOWLEDGE_BASE_DIR}")
    if LOG_DIR.exists() and not LOG_DIR.is_dir():
        raise NotADirectoryError(f"路径已存在但不是目录: {LOG_DIR}")
    KNOWLEDGE_BASE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
except OSError as e:
    import warnings
    warnings.warn(f"无法创建必要目录: {e}。部分功能可能不可用。")

# 知识图谱配置
KNOWLEDGE_GRAPH_DB = str(KNOWLEDGE_BASE_DIR / "knowledge.db")
EXPERIENCE_DIR = KNOWLEDGE_BASE_DIR / "experiences"
PATTERNS_DIR = KNOWLEDGE_BASE_DIR / "patterns"

# 执行配置
MAX_WORKERS = 3  # 最大并行任务数
TASK_TIMEOUT = 300  # 任务超时时间(秒)
RETRY_COUNT = 3  # 重试次数

# 保活配置
HEARTBEAT_INTERVAL = 30  # 心跳间隔(秒)
MAX_CRASH_COUNT = 5  # 最大崩溃次数(超过则报警)
CRASH_RESET_TIME = 300  # 崩溃计数重置时间(秒)

# 优化配置
OPTIMIZATION_INTERVAL = 60  # 优化检查间隔(秒)
MIN_EXPERIENCE_FOR_OPTIMIZATION = 5  # 触发优化的最小经验数

# 监控配置
WATCH_PATTERNS = ["*.py", "*.js", "*.ts", "*.java", "*.go", "*.rs"]
IGNORE_PATTERNS = ["__pycache__", "node_modules", ".git", "venv", "build", "dist"]

# 日志配置
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# 工作目录(运行时可覆盖)
WORK_DIR = os.getcwd()
