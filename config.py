"""
极简全能持续运行Agent - 配置文件
"""
import os
from pathlib import Path

# 基础路径
BASE_DIR = Path(__file__).parent.absolute()
KNOWLEDGE_BASE_DIR = BASE_DIR / "knowledge_base"
LOG_DIR = BASE_DIR / "logs"

# 确保目录存在
KNOWLEDGE_BASE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

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
