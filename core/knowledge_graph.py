"""
知识图谱模块 - 经验存储与检索
支持：经验存储、模式识别、快速检索、自动归类

R24修复:
- [R24-Bug#201] close() 方法对文件数据库也执行WAL checkpoint
- [R24-Bug#202] 添加 checkpoint_wal() 方法防止WAL文件无限增长
"""
import sqlite3
import json
import re
import uuid
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class Experience:
    """经验记录"""
    id: str
    task_type: str  # bug_fix, refactor, implement, analyze
    context: str    # 上下文描述
    problem: str    # 问题/需求
    solution: str   # 解决方案
    outcome: str    # 结果(success/failure)
    lessons: str    # 经验教训
    code_snippets: List[str]  # 相关代码片段
    tags: List[str] # 标签
    created_at: str
    success_rate: float = 1.0
    usage_count: int = 0

    def __post_init__(self):
        """验证字段值范围"""
        # [Bug#174修复] 验证必填字段不为None
        if self.problem is None:
            raise ValueError("problem must not be None")
        if self.task_type is None:
            raise ValueError("task_type must not be None")
        if self.solution is None:
            raise ValueError("solution must not be None")
        if self.outcome is None:
            raise ValueError("outcome must not be None")
        # [Bug#174修复] 验证列表字段不为None，空列表使用默认值
        if self.tags is None:
            self.tags = []
        if self.code_snippets is None:
            self.code_snippets = []
        if self.context is None:
            self.context = "{}"

        # 验证 success_rate 在 [0, 1] 范围内
        if not isinstance(self.success_rate, (int, float)):
            raise TypeError(f"success_rate must be a number, got {type(self.success_rate).__name__}")
        if not 0.0 <= self.success_rate <= 1.0:
            raise ValueError(f"success_rate must be in range [0, 1], got {self.success_rate}")

        # 验证 usage_count 为非负整数
        if not isinstance(self.usage_count, int):
            raise TypeError(f"usage_count must be an integer, got {type(self.usage_count).__name__}")
        if self.usage_count < 0:
            raise ValueError(f"usage_count must be non-negative, got {self.usage_count}")


@dataclass
class Pattern:
    """代码模式"""
    id: str
    pattern_type: str  # bug_pattern, best_practice, anti_pattern
    name: str
    description: str
    code_example: str
    when_to_use: str
    effectiveness: float
    created_at: str

    def __post_init__(self):
        """验证effectiveness范围在[0,1]之间"""
        if not isinstance(self.effectiveness, (int, float)):
            raise TypeError(f"effectiveness must be a number, got {type(self.effectiveness).__name__}")
        if not 0.0 <= self.effectiveness <= 1.0:
            raise ValueError(f"effectiveness must be in range [0, 1], got {self.effectiveness}")


class KnowledgeGraph:
    """
    知识图谱核心类
    使用SQLite存储结构化数据，支持快速检索和关联分析
    """

    # 指数移动平均的平滑因子
    _EMA_ALPHA = 0.3

    def __init__(self, db_path: str):
        logger.debug(f"[TRACE] __init__ called with db_path={db_path!r}")
        self.db_path = db_path
        self._lock = threading.RLock()
        # 对于内存数据库，保持持久连接
        self._is_memory_db = (db_path == ":memory:")
        self._persistent_conn = None
        # [H-线程安全] 保护 _persistent_conn 初始化的专用锁
        self._conn_lock = threading.Lock()
        logger.debug(f"[TRACE] __init__ is_memory_db={self._is_memory_db}, calling _init_database")
        self._init_database()
        logger.debug(f"[TRACE] __init__ completed for db_path={db_path!r}")

    def _get_connection(self):
        """获取数据库连接 [H-Bug修复] 添加busy_timeout防止高并发时database is locked"""
        if self._is_memory_db:
            logger.debug("[TRACE] _get_connection: memory DB path, acquiring _conn_lock")
            # [H-线程安全] 对 _persistent_conn 的初始化加锁保护
            with self._conn_lock:
                if self._persistent_conn is None:
                    logger.debug("[TRACE] _get_connection: creating new persistent connection for memory DB")
                    self._persistent_conn = sqlite3.connect(
                        self.db_path, check_same_thread=False
                    )
                    # 设置busy_timeout为30秒
                    self._persistent_conn.execute("PRAGMA busy_timeout=30000")
                    logger.debug("[TRACE] _get_connection: PRAGMA busy_timeout=30000 set on memory DB connection")
                else:
                    logger.debug("[TRACE] _get_connection: reusing existing persistent connection for memory DB")
            return self._persistent_conn
        # [H-Bug修复] 非内存数据库也使用连接池模式，设置busy_timeout
        logger.debug(f"[TRACE] _get_connection: non-memory DB, creating new connection for {self.db_path!r}")
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        logger.debug(f"[TRACE] _get_connection: new connection created and PRAGMA set for {self.db_path!r}")
        return conn

    def _close_connection(self, conn):
        """关闭非内存数据库连接"""
        if not self._is_memory_db and conn is not None:
            logger.debug("[TRACE] _close_connection: closing non-memory DB connection")
            conn.close()
        else:
            logger.debug("[TRACE] _close_connection: skipping close (memory DB or conn is None)")

    def _init_database(self):
        """初始化数据库表"""
        logger.debug("[TRACE] _init_database: starting")
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # 经验表
            logger.debug("[TRACE] _init_database: CREATE TABLE IF NOT EXISTS experiences")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS experiences (
                    id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    context TEXT,
                    problem TEXT NOT NULL,
                    solution TEXT,
                    outcome TEXT,
                    lessons TEXT,
                    code_snippets TEXT,
                    tags TEXT,
                    created_at TEXT,
                    success_rate REAL DEFAULT 1.0,
                    usage_count INTEGER DEFAULT 0
                )
            ''')

            # 模式表
            logger.debug("[TRACE] _init_database: CREATE TABLE IF NOT EXISTS patterns")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patterns (
                    id TEXT PRIMARY KEY,
                    pattern_type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    code_example TEXT,
                    when_to_use TEXT,
                    effectiveness REAL DEFAULT 0.5,
                    created_at TEXT
                )
            ''')

            # 关联表(经验-模式关联)
            logger.debug("[TRACE] _init_database: CREATE TABLE IF NOT EXISTS experience_patterns")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS experience_patterns (
                    experience_id TEXT,
                    pattern_id TEXT,
                    relevance REAL DEFAULT 1.0,
                    PRIMARY KEY (experience_id, pattern_id)
                )
            ''')

            # 标签索引表
            logger.debug("[TRACE] _init_database: CREATE TABLE IF NOT EXISTS tag_index")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tag_index (
                    tag TEXT PRIMARY KEY,
                    experience_ids TEXT,
                    pattern_ids TEXT
                )
            ''')

            # 创建索引
            logger.debug("[TRACE] _init_database: CREATE INDEX IF NOT EXISTS idx_exp_type")
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exp_type ON experiences(task_type)')
            logger.debug("[TRACE] _init_database: CREATE INDEX IF NOT EXISTS idx_exp_outcome")
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exp_outcome ON experiences(outcome)')
            logger.debug("[TRACE] _init_database: CREATE INDEX IF NOT EXISTS idx_pattern_type")
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pattern_type ON patterns(pattern_type)')

            logger.debug("[TRACE] _init_database: committing")
            conn.commit()
            logger.debug("[TRACE] _init_database: commit done")
        except Exception as e:
            logger.error(f"[TRACE][ERROR] _init_database failed: {e}", exc_info=True)
            # Clean up broken persistent connection
            if self._is_memory_db and self._persistent_conn is not None:
                try:
                    self._persistent_conn.close()
                except Exception:
                    pass
                self._persistent_conn = None
            raise
        finally:
            self._close_connection(conn)
        logger.info(f"知识图谱数据库初始化完成: {self.db_path}")
        logger.debug("[TRACE] _init_database: completed")

    def close(self):
        """显式关闭数据库连接，防止连接泄漏
        
        Bug#201修复: 对文件数据库也执行WAL checkpoint并关闭连接
        """
        logger.debug(f"[TRACE] close: called, is_memory_db={self._is_memory_db}")
        if self._is_memory_db:
            # 内存数据库: 关闭持久连接
            if self._persistent_conn is not None:
                logger.debug("[TRACE] close: closing persistent connection for memory DB")
                try:
                    self._persistent_conn.close()
                    logger.debug("[TRACE] close: persistent connection closed successfully")
                except Exception as e:
                    logger.error(f"[TRACE][ERROR] close: error closing connection: {e}")
                finally:
                    self._persistent_conn = None
        else:
            # [Bug#201修复] 文件数据库: 执行WAL checkpoint并关闭所有连接
            logger.debug("[TRACE] close: checkpointing and closing file DB connections")
            try:
                self.checkpoint_wal()
            except Exception as e:
                logger.warning(f"[TRACE] close: WAL checkpoint failed: {e}")
        logger.debug("[TRACE] close: completed")

    def checkpoint_wal(self):
        """[Bug#202修复] 执行WAL checkpoint，防止WAL文件无限增长"""
        if self._is_memory_db:
            return  # 内存数据库不需要checkpoint
        
        conn = None
        try:
            conn = self._get_connection()
            # TRUNCATE模式: checkpoint后将WAL文件截断为0字节
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.commit()
            logger.debug("[TRACE] checkpoint_wal: WAL checkpoint completed")
        except Exception as e:
            logger.warning(f"WAL checkpoint失败: {e}")
        finally:
            self._close_connection(conn)

    def __del__(self):
        """析构函数中关闭连接，确保资源释放"""
        logger.debug("[TRACE] __del__: called, attempting to close connection")
        try:
            self.close()
        except Exception:
            # 析构函数中不应抛出异常
            pass

    def _generate_id(self, content: str) -> str:
        """生成唯一ID，使用uuid避免碰撞"""
        return uuid.uuid4().hex[:12]

    @staticmethod
    def _escape_like(value: str) -> str:
        """转义SQL LIKE通配符中的特殊字符 % 和 _"""
        return value.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

    def add_experience(self, experience: Experience) -> str:
        """[H-Bug修复] 添加经验 - 先读取旧标签再执行INSERT，确保标签索引正确清理"""
        logger.debug(f"[TRACE][LOCK] acquire _lock in add_experience, experience_id={experience.id!r}")
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                
                # [H-Bug修复] 先读取旧标签（在INSERT之前）
                logger.debug(f"[TRACE] add_experience: querying old tags for experience_id={experience.id!r}")
                cursor.execute('SELECT tags FROM experiences WHERE id = ?', (experience.id,))
                old_row = cursor.fetchone()
                # [H-Bug修复] 安全解析旧标签
                try:
                    old_tags = json.loads(old_row[0]) if old_row and old_row[0] else []
                except (json.JSONDecodeError, TypeError):
                    old_tags = []
                logger.debug(f"[TRACE] add_experience: old_tags={old_tags!r} for experience_id={experience.id!r}")
                
                # 执行 INSERT ... ON CONFLICT DO UPDATE
                logger.debug(f"[TRACE] add_experience: executing INSERT ON CONFLICT for experience_id={experience.id!r}, task_type={experience.task_type!r}")
                # [Bug#196修复] ON CONFLICT时保留usage_count和success_rate，避免覆盖历史统计
                cursor.execute('''
                    INSERT INTO experiences
                        (id, task_type, context, problem, solution, outcome, lessons,
                         code_snippets, tags, created_at, success_rate, usage_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        task_type = excluded.task_type,
                        context = excluded.context,
                        problem = excluded.problem,
                        solution = excluded.solution,
                        outcome = excluded.outcome,
                        lessons = excluded.lessons,
                        code_snippets = excluded.code_snippets,
                        tags = excluded.tags,
                        created_at = excluded.created_at
                ''', (
                    experience.id,
                    experience.task_type,
                    experience.context,
                    experience.problem,
                    experience.solution,
                    experience.outcome,
                    experience.lessons,
                    json.dumps(experience.code_snippets, ensure_ascii=False),
                    json.dumps(experience.tags, ensure_ascii=False),
                    experience.created_at,
                    experience.success_rate,
                    experience.usage_count
                ))
                logger.debug(f"[TRACE] add_experience: INSERT ON CONFLICT done for experience_id={experience.id!r}")

                # 清理被移除的旧标签索引
                for old_tag in old_tags:
                    if old_tag not in experience.tags:
                        logger.debug(f"[TRACE] add_experience: removing old tag {old_tag!r} from index for experience_id={experience.id!r}")
                        self._remove_tag_index(cursor, old_tag, experience.id, 'experience')

                # 更新标签索引
                for tag in experience.tags:
                    logger.debug(f"[TRACE] add_experience: updating tag index for tag={tag!r}, experience_id={experience.id!r}")
                    self._update_tag_index(cursor, tag, experience.id, 'experience')

                logger.debug(f"[TRACE] add_experience: committing for experience_id={experience.id!r}")
                conn.commit()
                logger.debug(f"[TRACE] add_experience: commit done for experience_id={experience.id!r}")
            except Exception as e:
                logger.error(f"[TRACE][ERROR] add_experience failed for experience_id={experience.id!r}: {e}", exc_info=True)
                conn.rollback()
                raise
            finally:
                self._close_connection(conn)
        logger.debug(f"[TRACE][LOCK] release _lock in add_experience, experience_id={experience.id!r}")
        logger.info(f"添加经验: {experience.id} - {experience.task_type}")
        return experience.id

    def _update_tag_index(self, cursor, tag: str, item_id: str, item_type: str):
        """更新标签索引 - 使用SQLite JSON函数进行原子更新，避免竞态条件"""
        logger.debug(f"[TRACE] _update_tag_index: tag={tag!r}, item_id={item_id!r}, item_type={item_type!r}")

        # 先尝试INSERT，如果已存在则忽略
        cursor.execute('''
            INSERT OR IGNORE INTO tag_index (tag, experience_ids, pattern_ids) VALUES (?, '[]', '[]')
        ''', (tag,))

        # 使用SQLite JSON函数原子更新数组
        # [Bug#173修复] 使用json_valid保护，防止malformed JSON导致操作失败
        if item_type == 'experience':
            # 使用json_insert在数组末尾添加元素，但先检查是否已存在
            cursor.execute('''
                UPDATE tag_index
                SET experience_ids = CASE
                    WHEN json_valid(experience_ids) = 0 OR json_type(experience_ids) IS NULL THEN json_array(?)
                    WHEN json_array_length(experience_ids) = 0 THEN json_array(?)
                    WHEN json_type(experience_ids, '$') = 'array' THEN
                        CASE
                            WHEN ? IN (SELECT value FROM json_each(experience_ids)) THEN experience_ids
                            ELSE json_insert(experience_ids, '$[#]', ?)
                        END
                    ELSE json_array(?)
                END
                WHERE tag = ?
            ''', (item_id, item_id, item_id, item_id, item_id, tag))
        else:  # pattern
            cursor.execute('''
                UPDATE tag_index
                SET pattern_ids = CASE
                    WHEN json_valid(pattern_ids) = 0 OR json_type(pattern_ids) IS NULL THEN json_array(?)
                    WHEN json_array_length(pattern_ids) = 0 THEN json_array(?)
                    WHEN json_type(pattern_ids, '$') = 'array' THEN
                        CASE
                            WHEN ? IN (SELECT value FROM json_each(pattern_ids)) THEN pattern_ids
                            ELSE json_insert(pattern_ids, '$[#]', ?)
                        END
                    ELSE json_array(?)
                END
                WHERE tag = ?
            ''', (item_id, item_id, item_id, item_id, item_id, tag))

        logger.debug(f"[TRACE] _update_tag_index: atomic UPDATE done for tag={tag!r}, item_type={item_type!r}")

    def _remove_tag_index(self, cursor, tag: str, item_id: str, item_type: str):
        """从标签索引中移除指定项"""
        logger.debug(f"[TRACE] _remove_tag_index: tag={tag!r}, item_id={item_id!r}, item_type={item_type!r}")
        cursor.execute('SELECT experience_ids, pattern_ids FROM tag_index WHERE tag = ?', (tag,))
        row = cursor.fetchone()
        if not row:
            logger.debug(f"[TRACE] _remove_tag_index: tag={tag!r} not found in index, nothing to remove")
            return

        # [H-Bug修复] 安全解析JSON数组
        try:
            exp_ids = json.loads(row[0]) if row[0] else []
        except (json.JSONDecodeError, TypeError):
            exp_ids = []
        try:
            pat_ids = json.loads(row[1]) if row[1] else []
        except (json.JSONDecodeError, TypeError):
            pat_ids = []
        changed = False

        if item_type == 'experience' and item_id in exp_ids:
            exp_ids.remove(item_id)
            changed = True
        elif item_type == 'pattern' and item_id in pat_ids:
            pat_ids.remove(item_id)
            changed = True

        if changed:
            if exp_ids or pat_ids:
                logger.debug(f"[TRACE] _remove_tag_index: UPDATE tag_index for tag={tag!r}, remaining exp_ids={len(exp_ids)}, pat_ids={len(pat_ids)}")
                cursor.execute('''
                    UPDATE tag_index SET experience_ids = ?, pattern_ids = ? WHERE tag = ?
                ''', (json.dumps(exp_ids), json.dumps(pat_ids), tag))
            else:
                logger.debug(f"[TRACE] _remove_tag_index: DELETE empty tag from index: tag={tag!r}")
                cursor.execute('DELETE FROM tag_index WHERE tag = ?', (tag,))
        else:
            logger.debug(f"[TRACE] _remove_tag_index: item_id={item_id!r} not found in tag={tag!r}, no change")

    def add_pattern(self, pattern: Pattern) -> str:
        """[H-Bug修复] 添加模式 - 改用INSERT ON CONFLICT避免孤儿记录，清理旧标签"""
        logger.debug(f"[TRACE][LOCK] acquire _lock in add_pattern, pattern_id={pattern.id!r}")
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                
                # [H-Bug修复] 先读取旧pattern的标签信息（在INSERT之前）
                logger.debug(f"[TRACE] add_pattern: querying old pattern for pattern_id={pattern.id!r}")
                cursor.execute('SELECT pattern_type, name FROM patterns WHERE id = ?', (pattern.id,))
                old_row = cursor.fetchone()
                old_tags = []
                if old_row:
                    old_tags = [old_row[0], old_row[1]]  # pattern_type, name
                logger.debug(f"[TRACE] add_pattern: old_tags={old_tags!r} for pattern_id={pattern.id!r}")
                
                # [H-Bug修复] 改用 INSERT ... ON CONFLICT DO UPDATE 替代 INSERT OR REPLACE
                # 避免 experience_patterns 表产生孤儿记录
                logger.debug(f"[TRACE] add_pattern: executing INSERT ON CONFLICT for pattern_id={pattern.id!r}, name={pattern.name!r}")
                cursor.execute('''
                    INSERT INTO patterns
                        (id, pattern_type, name, description, code_example, when_to_use, effectiveness, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        pattern_type = excluded.pattern_type,
                        name = excluded.name,
                        description = excluded.description,
                        code_example = excluded.code_example,
                        when_to_use = excluded.when_to_use,
                        effectiveness = excluded.effectiveness,
                        created_at = excluded.created_at
                ''', (
                    pattern.id, pattern.pattern_type, pattern.name, pattern.description,
                    pattern.code_example, pattern.when_to_use, pattern.effectiveness, pattern.created_at
                ))
                logger.debug(f"[TRACE] add_pattern: INSERT ON CONFLICT done for pattern_id={pattern.id!r}")

                # [H-Bug修复] 清理旧标签索引
                new_tags = [pattern.pattern_type, pattern.name]
                for old_tag in old_tags:
                    if old_tag not in new_tags:
                        logger.debug(f"[TRACE] add_pattern: removing old tag {old_tag!r} from index for pattern_id={pattern.id!r}")
                        self._remove_tag_index(cursor, old_tag, pattern.id, 'pattern')

                # 更新标签索引
                for tag in new_tags:
                    logger.debug(f"[TRACE] add_pattern: updating tag index for tag={tag!r}, pattern_id={pattern.id!r}")
                    self._update_tag_index(cursor, tag, pattern.id, 'pattern')

                # [M-Bug#122修复] 根据标签自动关联相关经验到 experience_patterns 表
                for tag in new_tags:
                    cursor.execute(
                        'SELECT experience_ids FROM tag_index WHERE tag = ?', (tag,)
                    )
                    row = cursor.fetchone()
                    if row and row[0]:
                        # [H-Bug修复] 安全解析经验ID列表
                        try:
                            exp_ids = json.loads(row[0])
                        except (json.JSONDecodeError, TypeError):
                            exp_ids = []
                        for exp_id in exp_ids:
                            cursor.execute('''
                                INSERT OR IGNORE INTO experience_patterns (experience_id, pattern_id, relevance)
                                VALUES (?, ?, 1.0)
                            ''', (exp_id, pattern.id))

                logger.debug(f"[TRACE] add_pattern: committing for pattern_id={pattern.id!r}")
                conn.commit()
                logger.debug(f"[TRACE] add_pattern: commit done for pattern_id={pattern.id!r}")
            except Exception as e:
                logger.error(f"[TRACE][ERROR] add_pattern failed for pattern_id={pattern.id!r}: {e}", exc_info=True)
                conn.rollback()
                raise
            finally:
                self._close_connection(conn)
        logger.debug(f"[TRACE][LOCK] release _lock in add_pattern, pattern_id={pattern.id!r}")
        logger.info(f"添加模式: {pattern.id} - {pattern.name}")
        return pattern.id

    def search_experiences(self, query: str, task_type: Optional[str] = None,
                          tags: Optional[List[str]] = None, limit: int = 10) -> List[Experience]:
        """搜索相关经验"""
        limit = max(1, min(limit, 1000))
        logger.debug(f"[TRACE][LOCK] acquire _lock in search_experiences, query={query!r}, task_type={task_type!r}, tags={tags!r}, limit={limit}")
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                sql = "SELECT * FROM experiences WHERE 1=1"
                params = []

                # [M-读操作加锁] 对内存数据库也需要锁保护，已由外层 with self._lock 保证

                # 关键词搜索
                if query:
                    sql += " AND (problem LIKE ? ESCAPE '\\' OR solution LIKE ? ESCAPE '\\' OR context LIKE ? ESCAPE '\\' OR lessons LIKE ? ESCAPE '\\')"
                    # [M-LIKE转义] 转义 % 和 _ 通配符
                    escaped = self._escape_like(query)
                    query_param = f"%{escaped}%"
                    params.extend([query_param, query_param, query_param, query_param])

                # 任务类型过滤
                if task_type:
                    sql += " AND task_type = ?"
                    params.append(task_type)

                # [Bug#173修复] 标签过滤：使用json_valid保护，防止malformed JSON导致整个查询崩溃
                # json_type对malformed JSON会直接抛异常，json_valid返回0/1更安全
                if tags:
                    for tag in tags:
                        sql += " AND json_valid(experiences.tags) AND json_type(experiences.tags) = 'array' AND EXISTS (SELECT 1 FROM json_each(experiences.tags) WHERE json_each.value = ?)"
                        params.append(tag)

                # 按成功率和使用次数排序
                sql += " ORDER BY success_rate DESC, usage_count DESC LIMIT ?"
                params.append(limit)

                logger.debug(f"[TRACE] search_experiences: executing SQL, params_count={len(params)}, limit={limit}")
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                logger.debug(f"[TRACE] search_experiences: query returned {len(rows)} rows")

                return [self._row_to_experience(row) for row in rows]
            finally:
                logger.debug(f"[TRACE][LOCK] release _lock in search_experiences")
                self._close_connection(conn)

    def _row_to_experience(self, row) -> Experience:
        """数据库行转Experience对象"""
        # [H-Bug修复] 安全解析JSON字段，处理损坏数据
        def safe_json_loads(data, default=None):
            if not data:
                return default if default is not None else []
            try:
                return json.loads(data)
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"[TRACE][ERROR] JSON解析失败，使用默认值: {e}")
                return default if default is not None else []
        
        # [H-Bug修复] 修复损坏的数值字段
        success_rate = row[10] if row[10] is not None else 1.0
        if not isinstance(success_rate, (int, float)) or success_rate < 0 or success_rate > 1:
            logger.warning(f"[TRACE][ERROR] success_rate超出范围，修正为1.0: {success_rate}")
            success_rate = 1.0
        
        usage_count = row[11] if row[11] is not None else 0
        if not isinstance(usage_count, int) or usage_count < 0:
            logger.warning(f"[TRACE][ERROR] usage_count无效，修正为0: {usage_count}")
            usage_count = 0
        
        return Experience(
            id=row[0],
            task_type=row[1],
            context=row[2],
            problem=row[3],
            solution=row[4],
            outcome=row[5],
            lessons=row[6],
            code_snippets=safe_json_loads(row[7], []),
            tags=safe_json_loads(row[8], []),
            created_at=row[9],
            success_rate=success_rate,
            usage_count=usage_count
        )

    def get_patterns_by_type(self, pattern_type: str, min_effectiveness: float = 0.5) -> List[Pattern]:
        """获取指定类型的模式"""
        logger.debug(f"[TRACE][LOCK] acquire _lock in get_patterns_by_type, pattern_type={pattern_type!r}, min_effectiveness={min_effectiveness}")
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                logger.debug(f"[TRACE] get_patterns_by_type: executing query, pattern_type={pattern_type!r}, min_effectiveness={min_effectiveness}")
                cursor.execute('''
                    SELECT * FROM patterns
                    WHERE pattern_type = ? AND effectiveness >= ?
                    ORDER BY effectiveness DESC
                ''', (pattern_type, min_effectiveness))
                rows = cursor.fetchall()
                logger.debug(f"[TRACE] get_patterns_by_type: query returned {len(rows)} rows")

                return [Pattern(
                    id=row[0], pattern_type=row[1], name=row[2], description=row[3],
                    code_example=row[4], when_to_use=row[5], effectiveness=row[6], created_at=row[7]
                ) for row in rows]
            finally:
                logger.debug(f"[TRACE][LOCK] release _lock in get_patterns_by_type")
                self._close_connection(conn)

    def update_experience_usage(self, experience_id: str, success: bool):
        """更新经验使用统计"""
        logger.debug(f"[TRACE][LOCK] acquire _lock in update_experience_usage, experience_id={experience_id!r}, success={success}")
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                logger.debug(f"[TRACE] update_experience_usage: querying existing stats for experience_id={experience_id!r}")
                cursor.execute('''
                    SELECT usage_count, success_rate FROM experiences WHERE id = ?
                ''', (experience_id,))
                row = cursor.fetchone()

                if row:
                    usage_count = row[0] + 1
                    # [M-成功率更新] 使用指数移动平均(EMA)代替简单滑动窗口
                    old_rate = row[1]
                    # 验证old_rate合法性
                    if old_rate is None or not isinstance(old_rate, (int, float)) or old_rate < 0 or old_rate > 1:
                        logger.warning(f"[TRACE][ERROR] update_experience_usage: old_rate非法，使用默认值0.5: {old_rate}")
                        old_rate = 0.5
                    alpha = self._EMA_ALPHA
                    new_rate = alpha * (1.0 if success else 0.0) + (1 - alpha) * old_rate
                    logger.debug(f"[TRACE] update_experience_usage: experience_id={experience_id!r}, usage_count={row[0]}->{usage_count}, old_rate={old_rate:.4f}, new_rate={new_rate:.4f}, success={success}")

                    cursor.execute('''
                        UPDATE experiences SET usage_count = ?, success_rate = ? WHERE id = ?
                    ''', (usage_count, new_rate, experience_id))
                    logger.debug(f"[TRACE] update_experience_usage: UPDATE done for experience_id={experience_id!r}")
                    conn.commit()
                    logger.debug(f"[TRACE] update_experience_usage: commit done for experience_id={experience_id!r}")
                else:
                    logger.warning(f"[TRACE] update_experience_usage: experience_id={experience_id!r} not found, skipping update")
            except Exception as e:
                logger.error(f"[TRACE][ERROR] update_experience_usage failed for experience_id={experience_id!r}: {e}", exc_info=True)
                raise
            finally:
                self._close_connection(conn)
        logger.debug(f"[TRACE][LOCK] release _lock in update_experience_usage, experience_id={experience_id!r}")

    def _tokenize(self, text: str) -> set:
        """
        分词：支持中英文混合文本
        英文按空格分词，中文使用字符级n-gram作为后备
        """
        if text is None:
            return set()
        tokens = set()
        # 英文分词：按空格和标点分割
        words = re.findall(r'[a-zA-Z0-9]+', text.lower())
        tokens.update(w for w in words if len(w) > 1)

        # [L-中文分词] 提取中文字符，生成字符级bigram和unigram
        chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
        if chinese_chars:
            # unigram
            tokens.update(chinese_chars)
            # bigram
            for i in range(len(chinese_chars) - 1):
                tokens.add(chinese_chars[i] + chinese_chars[i + 1])

        return tokens

    def get_similar_experiences(self, problem: str, limit: int = 5) -> List[Tuple[Experience, float]]:
        """获取相似经验(基于文本相似度) - V3改进版
        
        改进：不再依赖LIKE搜索，直接获取所有经验计算相似度
        这样可以更好地处理中文分词和语义匹配
        """
        limit = max(1, min(limit, 1000))
        logger.debug(f"[TRACE] get_similar_experiences: called with problem={problem!r}, limit={limit}")
        # 如果problem为空，返回空列表
        if not problem or not problem.strip():
            logger.debug("[TRACE] get_similar_experiences: problem is empty, returning []")
            return []

        # 使用改进的分词方法
        problem_tokens = self._tokenize(problem)
        logger.debug(f"[TRACE] get_similar_experiences: tokenized problem, token_count={len(problem_tokens)}")
        if not problem_tokens:
            logger.debug("[TRACE] get_similar_experiences: no tokens produced, returning []")
            return []

        # 直接从数据库获取所有经验（不使用LIKE预过滤）
        # 这样可以避免LIKE匹配不到的问题
        logger.debug(f"[TRACE][LOCK] acquire _lock in get_similar_experiences")
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                # 获取最近的经验，按使用次数排序
                logger.debug("[TRACE] get_similar_experiences: querying top 200 experiences by usage_count")
                cursor.execute('''
                    SELECT * FROM experiences 
                    ORDER BY usage_count DESC, success_rate DESC 
                    LIMIT 200
                ''')
                rows = cursor.fetchall()
                experiences = [self._row_to_experience(row) for row in rows]
                logger.debug(f"[TRACE] get_similar_experiences: fetched {len(experiences)} experiences from DB")
            finally:
                self._close_connection(conn)
        logger.debug(f"[TRACE][LOCK] release _lock in get_similar_experiences")

        if not experiences:
            logger.debug("[TRACE] get_similar_experiences: no experiences in DB, returning []")
            return []

        scored = []
        for exp in experiences:
            # [M-Bug修复] 构建经验文本时过滤None值，避免"None"字符串干扰分词
            parts = [exp.problem or "", exp.solution or "", exp.context or "", exp.lessons or ""]
            exp_text = " ".join(p for p in parts if p)
            exp_tokens = self._tokenize(exp_text)

            if not exp_tokens:
                continue

            # 计算Jaccard相似度
            common = problem_tokens & exp_tokens
            if not common:
                continue
                
            union = problem_tokens | exp_tokens
            jaccard = len(common) / len(union) if union else 0

            # 额外加分：直接包含关键词（对中文更友好）
            bonus = 0
            exp_text_lower = exp_text.lower()
            problem_lower = problem.lower()
            
            # 检查问题文本是否直接出现在经验文本中
            if problem_lower in exp_text_lower:
                bonus += 0.3
            
            # 检查每个token
            for token in problem_tokens:
                if len(token) > 1 and token in exp_text_lower:
                    bonus += 0.05

            # [M-Bug修复] bonus设置上限，防止区分度下降
            bonus = min(bonus, 0.3)
            similarity = min(jaccard + bonus, 1.0)

            if similarity > 0.01:  # 进一步降低阈值
                scored.append((exp, similarity))

        # 按相似度排序
        scored.sort(key=lambda x: x[1], reverse=True)
        logger.debug(f"[TRACE] get_similar_experiences: scored {len(scored)} experiences above threshold")

        # 如果没有匹配到，返回最常用的经验
        if not scored and experiences:
            logger.debug(f"[TRACE] get_similar_experiences: no scored matches, returning top {limit} by usage with score=0.1")
            return [(exp, 0.1) for exp in experiences[:limit]]

        logger.debug(f"[TRACE] get_similar_experiences: returning top {min(limit, len(scored))} results")
        return scored[:limit]

    def get_statistics(self) -> Dict[str, Any]:
        """获取知识库统计"""
        logger.debug(f"[TRACE][LOCK] acquire _lock in get_statistics")
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                logger.debug("[TRACE] get_statistics: querying experience count")
                cursor.execute('SELECT COUNT(*) FROM experiences')
                exp_count = cursor.fetchone()[0]

                logger.debug("[TRACE] get_statistics: querying pattern count")
                cursor.execute('SELECT COUNT(*) FROM patterns')
                pattern_count = cursor.fetchone()[0]

                logger.debug("[TRACE] get_statistics: querying experiences by task_type")
                cursor.execute('SELECT task_type, COUNT(*) FROM experiences GROUP BY task_type')
                by_type = dict(cursor.fetchall())

                logger.debug("[TRACE] get_statistics: querying average success_rate")
                cursor.execute('SELECT AVG(success_rate) FROM experiences')
                avg_success = cursor.fetchone()[0] or 0

                logger.debug(f"[TRACE] get_statistics: exp_count={exp_count}, pattern_count={pattern_count}, avg_success={avg_success}")
                return {
                    "total_experiences": exp_count,
                    "total_patterns": pattern_count,
                    "by_task_type": by_type,
                    "average_success_rate": round(avg_success, 3),
                    "last_updated": datetime.now().isoformat()
                }
            finally:
                logger.debug(f"[TRACE][LOCK] release _lock in get_statistics")
                self._close_connection(conn)

    def _get_statistics_with_cursor(self, cursor) -> Dict[str, Any]:
        """使用已有cursor获取统计信息，避免嵌套连接"""
        cursor.execute('SELECT COUNT(*) FROM experiences')
        exp_count = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM patterns')
        pattern_count = cursor.fetchone()[0]

        cursor.execute('SELECT task_type, COUNT(*) FROM experiences GROUP BY task_type')
        by_type = dict(cursor.fetchall())

        cursor.execute('SELECT AVG(success_rate) FROM experiences')
        avg_success = cursor.fetchone()[0] or 0

        return {
            "total_experiences": exp_count,
            "total_patterns": pattern_count,
            "by_task_type": by_type,
            "average_success_rate": round(avg_success, 3),
            "last_updated": datetime.now().isoformat()
        }

    def export_knowledge(self, output_path: str):
        """导出知识库"""
        logger.debug(f"[TRACE][LOCK] acquire _lock in export_knowledge, output_path={output_path!r}")
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                logger.debug("[TRACE] export_knowledge: querying all experiences")
                cursor.execute('SELECT * FROM experiences')
                experiences = [self._row_to_experience(row) for row in cursor.fetchall()]
                logger.debug(f"[TRACE] export_knowledge: fetched {len(experiences)} experiences")

                logger.debug("[TRACE] export_knowledge: querying all patterns")
                cursor.execute('SELECT * FROM patterns')
                patterns = [Pattern(
                    id=row[0], pattern_type=row[1], name=row[2], description=row[3],
                    code_example=row[4], when_to_use=row[5], effectiveness=row[6], created_at=row[7]
                ) for row in cursor.fetchall()]
                logger.debug(f"[TRACE] export_knowledge: fetched {len(patterns)} patterns")

                # [M-嵌套连接] 使用已有cursor获取统计信息，避免嵌套连接
                statistics = self._get_statistics_with_cursor(cursor)

                export_data = {
                    "experiences": [asdict(e) for e in experiences],
                    "patterns": [asdict(p) for p in patterns],
                    "statistics": statistics,
                    "exported_at": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"[TRACE][ERROR] export_knowledge failed for output_path={output_path!r}: {e}", exc_info=True)
                raise
            finally:
                self._close_connection(conn)

        logger.debug(f"[TRACE][LOCK] release _lock in export_knowledge")

        # File I/O outside the lock
        logger.debug(f"[TRACE] export_knowledge: writing to file {output_path!r}")
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        logger.debug(f"[TRACE] export_knowledge: file written successfully")
        logger.info(f"知识库已导出到: {output_path}")


# 全局知识图谱实例
_knowledge_graph: Optional[KnowledgeGraph] = None
# [H-线程安全] 保护全局单例初始化的锁
_kg_lock = threading.Lock()


def get_knowledge_graph(db_path: Optional[str] = None) -> KnowledgeGraph:
    """获取知识图谱单例"""
    global _knowledge_graph
    logger.debug(f"[TRACE] get_knowledge_graph: called with db_path={db_path!r}, current singleton={'exists' if _knowledge_graph is not None else 'None'}")
    # [H-线程安全] 使用双重检查锁定模式保护单例初始化
    if _knowledge_graph is None:
        logger.debug("[TRACE] get_knowledge_graph: first check passed (singleton is None), acquiring _kg_lock")
        with _kg_lock:
            if _knowledge_graph is None:
                logger.debug("[TRACE] get_knowledge_graph: second check passed, creating new KnowledgeGraph instance")
                # [M-运行时导入] 将 config 导入移到此处（延迟导入），保持文件顶部干净
                # 如果调用方已通过 db_path 参数指定路径，则无需导入 config
                if db_path is None:
                    from config import KNOWLEDGE_GRAPH_DB
                    db_path = KNOWLEDGE_GRAPH_DB
                    logger.debug(f"[TRACE] get_knowledge_graph: loaded db_path from config: {db_path!r}")
                _knowledge_graph = KnowledgeGraph(db_path)
                logger.debug(f"[TRACE] get_knowledge_graph: singleton created with db_path={db_path!r}")
            else:
                logger.debug("[TRACE] get_knowledge_graph: second check failed (singleton already created by another thread)")
    else:
        logger.debug("[TRACE] get_knowledge_graph: returning existing singleton")
    return _knowledge_graph
