"""
知识图谱模块 - 经验存储与检索
支持：经验存储、模式识别、快速检索、自动归类
"""
import sqlite3
import json
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


class KnowledgeGraph:
    """
    知识图谱核心类
    使用SQLite存储结构化数据，支持快速检索和关联分析
    """

    # 指数移动平均的平滑因子
    _EMA_ALPHA = 0.3

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        # 对于内存数据库，保持持久连接
        self._is_memory_db = (db_path == ":memory:")
        self._persistent_conn = None
        # [H-线程安全] 保护 _persistent_conn 初始化的专用锁
        self._conn_lock = threading.Lock()
        self._init_database()

    def _get_connection(self):
        """获取数据库连接 [H-Bug修复] 添加busy_timeout防止高并发时database is locked"""
        if self._is_memory_db:
            # [H-线程安全] 对 _persistent_conn 的初始化加锁保护
            with self._conn_lock:
                if self._persistent_conn is None:
                    self._persistent_conn = sqlite3.connect(
                        self.db_path, check_same_thread=False
                    )
                    # 设置busy_timeout为30秒
                    self._persistent_conn.execute("PRAGMA busy_timeout=30000")
            return self._persistent_conn
        # [H-Bug修复] 非内存数据库也使用连接池模式，设置busy_timeout
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _close_connection(self, conn):
        """关闭非内存数据库连接"""
        if not self._is_memory_db and conn is not None:
            conn.close()

    def _init_database(self):
        """初始化数据库表"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # 经验表
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
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS experience_patterns (
                    experience_id TEXT,
                    pattern_id TEXT,
                    relevance REAL DEFAULT 1.0,
                    PRIMARY KEY (experience_id, pattern_id)
                )
            ''')

            # 标签索引表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tag_index (
                    tag TEXT PRIMARY KEY,
                    experience_ids TEXT,
                    pattern_ids TEXT
                )
            ''')

            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exp_type ON experiences(task_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exp_outcome ON experiences(outcome)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pattern_type ON patterns(pattern_type)')

            conn.commit()
        finally:
            self._close_connection(conn)
        logger.info(f"知识图谱数据库初始化完成: {self.db_path}")

    def _generate_id(self, content: str) -> str:
        """生成唯一ID，使用uuid避免碰撞"""
        return uuid.uuid4().hex[:12]

    @staticmethod
    def _escape_like(value: str) -> str:
        """转义SQL LIKE通配符中的特殊字符 % 和 _"""
        return value.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

    def add_experience(self, experience: Experience) -> str:
        """[H-Bug修复] 添加经验 - 先读取旧标签再执行INSERT，确保标签索引正确清理"""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                
                # [H-Bug修复] 先读取旧标签（在INSERT之前）
                cursor.execute('SELECT tags FROM experiences WHERE id = ?', (experience.id,))
                old_row = cursor.fetchone()
                old_tags = json.loads(old_row[0]) if old_row and old_row[0] else []
                
                # 执行 INSERT ... ON CONFLICT DO UPDATE
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

                # 清理被移除的旧标签索引
                for old_tag in old_tags:
                    if old_tag not in experience.tags:
                        self._remove_tag_index(cursor, old_tag, experience.id, 'experience')

                # 更新标签索引
                for tag in experience.tags:
                    self._update_tag_index(cursor, tag, experience.id, 'experience')

                conn.commit()
            finally:
                self._close_connection(conn)
        logger.info(f"添加经验: {experience.id} - {experience.task_type}")
        return experience.id

    def _update_tag_index(self, cursor, tag: str, item_id: str, item_type: str):
        """更新标签索引"""
        cursor.execute('SELECT experience_ids, pattern_ids FROM tag_index WHERE tag = ?', (tag,))
        row = cursor.fetchone()

        if row:
            exp_ids = json.loads(row[0]) if row[0] else []
            pat_ids = json.loads(row[1]) if row[1] else []

            if item_type == 'experience' and item_id not in exp_ids:
                exp_ids.append(item_id)
            elif item_type == 'pattern' and item_id not in pat_ids:
                pat_ids.append(item_id)

            cursor.execute('''
                UPDATE tag_index SET experience_ids = ?, pattern_ids = ? WHERE tag = ?
            ''', (json.dumps(exp_ids), json.dumps(pat_ids), tag))
        else:
            exp_ids = [item_id] if item_type == 'experience' else []
            pat_ids = [item_id] if item_type == 'pattern' else []
            cursor.execute('''
                INSERT INTO tag_index (tag, experience_ids, pattern_ids) VALUES (?, ?, ?)
            ''', (tag, json.dumps(exp_ids), json.dumps(pat_ids)))

    def _remove_tag_index(self, cursor, tag: str, item_id: str, item_type: str):
        """从标签索引中移除指定项"""
        cursor.execute('SELECT experience_ids, pattern_ids FROM tag_index WHERE tag = ?', (tag,))
        row = cursor.fetchone()
        if not row:
            return

        exp_ids = json.loads(row[0]) if row[0] else []
        pat_ids = json.loads(row[1]) if row[1] else []
        changed = False

        if item_type == 'experience' and item_id in exp_ids:
            exp_ids.remove(item_id)
            changed = True
        elif item_type == 'pattern' and item_id in pat_ids:
            pat_ids.remove(item_id)
            changed = True

        if changed:
            if exp_ids or pat_ids:
                cursor.execute('''
                    UPDATE tag_index SET experience_ids = ?, pattern_ids = ? WHERE tag = ?
                ''', (json.dumps(exp_ids), json.dumps(pat_ids), tag))
            else:
                cursor.execute('DELETE FROM tag_index WHERE tag = ?', (tag,))

    def add_pattern(self, pattern: Pattern) -> str:
        """[H-Bug修复] 添加模式 - 改用INSERT ON CONFLICT避免孤儿记录，清理旧标签"""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                
                # [H-Bug修复] 先读取旧pattern的标签信息（在INSERT之前）
                cursor.execute('SELECT pattern_type, name FROM patterns WHERE id = ?', (pattern.id,))
                old_row = cursor.fetchone()
                old_tags = []
                if old_row:
                    old_tags = [old_row[0], old_row[1]]  # pattern_type, name
                
                # [H-Bug修复] 改用 INSERT ... ON CONFLICT DO UPDATE 替代 INSERT OR REPLACE
                # 避免 experience_patterns 表产生孤儿记录
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

                # [H-Bug修复] 清理旧标签索引
                new_tags = [pattern.pattern_type, pattern.name]
                for old_tag in old_tags:
                    if old_tag not in new_tags:
                        self._remove_tag_index(cursor, old_tag, pattern.id, 'pattern')

                # 更新标签索引
                for tag in new_tags:
                    self._update_tag_index(cursor, tag, pattern.id, 'pattern')

                conn.commit()
            finally:
                self._close_connection(conn)
        logger.info(f"添加模式: {pattern.id} - {pattern.name}")
        return pattern.id

    def search_experiences(self, query: str, task_type: Optional[str] = None,
                          tags: Optional[List[str]] = None, limit: int = 10) -> List[Experience]:
        """搜索相关经验"""
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

                # 标签过滤
                if tags:
                    for tag in tags:
                        sql += " AND tags LIKE ? ESCAPE '\\'"
                        # 转义标签中的通配符
                        escaped_tag = self._escape_like(tag)
                        params.append(f'%"{escaped_tag}"%')

                # 按成功率和使用次数排序
                sql += " ORDER BY success_rate DESC, usage_count DESC LIMIT ?"
                params.append(limit)

                cursor.execute(sql, params)
                rows = cursor.fetchall()

                return [self._row_to_experience(row) for row in rows]
            finally:
                self._close_connection(conn)

    def _row_to_experience(self, row) -> Experience:
        """数据库行转Experience对象"""
        return Experience(
            id=row[0],
            task_type=row[1],
            context=row[2],
            problem=row[3],
            solution=row[4],
            outcome=row[5],
            lessons=row[6],
            code_snippets=json.loads(row[7]) if row[7] else [],
            tags=json.loads(row[8]) if row[8] else [],
            created_at=row[9],
            success_rate=row[10],
            usage_count=row[11]
        )

    def get_patterns_by_type(self, pattern_type: str, min_effectiveness: float = 0.5) -> List[Pattern]:
        """获取指定类型的模式"""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM patterns
                    WHERE pattern_type = ? AND effectiveness >= ?
                    ORDER BY effectiveness DESC
                ''', (pattern_type, min_effectiveness))
                rows = cursor.fetchall()

                return [Pattern(
                    id=row[0], pattern_type=row[1], name=row[2], description=row[3],
                    code_example=row[4], when_to_use=row[5], effectiveness=row[6], created_at=row[7]
                ) for row in rows]
            finally:
                self._close_connection(conn)

    def update_experience_usage(self, experience_id: str, success: bool):
        """更新经验使用统计"""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT usage_count, success_rate FROM experiences WHERE id = ?
                ''', (experience_id,))
                row = cursor.fetchone()

                if row:
                    usage_count = row[0] + 1
                    # [M-成功率更新] 使用指数移动平均(EMA)代替简单滑动窗口
                    old_rate = row[1]
                    alpha = self._EMA_ALPHA
                    new_rate = alpha * (1.0 if success else 0.0) + (1 - alpha) * old_rate

                    cursor.execute('''
                        UPDATE experiences SET usage_count = ?, success_rate = ? WHERE id = ?
                    ''', (usage_count, new_rate, experience_id))
                    conn.commit()
            finally:
                self._close_connection(conn)

    def _tokenize(self, text: str) -> set:
        """
        分词：支持中英文混合文本
        英文按空格分词，中文使用字符级n-gram作为后备
        """
        tokens = set()
        # 英文分词：按空格和标点分割
        import re
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
        # 如果problem为空，返回空列表
        if not problem or not problem.strip():
            return []

        # 使用改进的分词方法
        problem_tokens = self._tokenize(problem)
        if not problem_tokens:
            return []

        # 直接从数据库获取所有经验（不使用LIKE预过滤）
        # 这样可以避免LIKE匹配不到的问题
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                # 获取最近的经验，按使用次数排序
                cursor.execute('''
                    SELECT * FROM experiences 
                    ORDER BY usage_count DESC, success_rate DESC 
                    LIMIT 200
                ''')
                rows = cursor.fetchall()
                experiences = [self._row_to_experience(row) for row in rows]
            finally:
                self._close_connection(conn)

        if not experiences:
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

        # 如果没有匹配到，返回最常用的经验
        if not scored and experiences:
            return [(exp, 0.1) for exp in experiences[:limit]]

        return scored[:limit]

    def get_statistics(self) -> Dict[str, Any]:
        """获取知识库统计"""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

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
            finally:
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
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                cursor.execute('SELECT * FROM experiences')
                experiences = [self._row_to_experience(row) for row in cursor.fetchall()]

                cursor.execute('SELECT * FROM patterns')
                patterns = [Pattern(
                    id=row[0], pattern_type=row[1], name=row[2], description=row[3],
                    code_example=row[4], when_to_use=row[5], effectiveness=row[6], created_at=row[7]
                ) for row in cursor.fetchall()]

                # [M-嵌套连接] 使用已有cursor获取统计信息，避免嵌套连接
                statistics = self._get_statistics_with_cursor(cursor)

                export_data = {
                    "experiences": [asdict(e) for e in experiences],
                    "patterns": [asdict(p) for p in patterns],
                    "statistics": statistics,
                    "exported_at": datetime.now().isoformat()
                }

                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, ensure_ascii=False, indent=2)
            finally:
                self._close_connection(conn)

        logger.info(f"知识库已导出到: {output_path}")


# 全局知识图谱实例
_knowledge_graph: Optional[KnowledgeGraph] = None
# [H-线程安全] 保护全局单例初始化的锁
_kg_lock = threading.Lock()


def get_knowledge_graph(db_path: Optional[str] = None) -> KnowledgeGraph:
    """获取知识图谱单例"""
    global _knowledge_graph
    # [H-线程安全] 使用双重检查锁定模式保护单例初始化
    if _knowledge_graph is None:
        with _kg_lock:
            if _knowledge_graph is None:
                # [M-运行时导入] 将 config 导入移到此处（延迟导入），保持文件顶部干净
                # 如果调用方已通过 db_path 参数指定路径，则无需导入 config
                if db_path is None:
                    from config import KNOWLEDGE_GRAPH_DB
                    db_path = KNOWLEDGE_GRAPH_DB
                _knowledge_graph = KnowledgeGraph(db_path)
    return _knowledge_graph
