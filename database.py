# database.py - 增强版数据库管理器
import sqlite3
import threading
import logging
from datetime import datetime
from pathlib import Path
import json
import hashlib

logger = logging.getLogger(__name__)

class EnhancedDatabaseManager:
    def __init__(self, db_path=None):
        if db_path:
            self.db_path = Path(db_path)
        else:
            self.db_path = Path(__file__).parent / 'database' / 'meituan.db'
        
        # 确保目录存在
        self.db_path.parent.mkdir(exist_ok=True)
        
        self._local = threading.local()
        self.init_database()
    
    def get_connection(self):
        """获取数据库连接（线程安全）"""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                str(self.db_path),
                timeout=30,
                check_same_thread=False
            )
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection
    
    def init_database(self):
        """初始化数据库表结构"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # 餐厅主表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS restaurants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT,
                district TEXT,
                business_district TEXT,
                category TEXT,
                phone TEXT,
                rating REAL CHECK(rating >= 0 AND rating <= 5),
                monthly_sales INTEGER CHECK(monthly_sales >= 0),
                address TEXT,
                crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fingerprint TEXT,  -- 用于去重的指纹
                phone_valid BOOLEAN DEFAULT 0,
                is_brand BOOLEAN DEFAULT 0,
                brand_name TEXT
            )
        ''')
        
        # 创建索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_restaurants_city ON restaurants(city)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_restaurants_rating ON restaurants(rating)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_restaurants_sales ON restaurants(monthly_sales)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_restaurants_crawl_time ON restaurants(crawl_time)
        ''')
        
        # 品牌配置表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS brand_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                aliases TEXT,  -- JSON格式存储别名列表
                filter_action TEXT CHECK(filter_action IN ('collect', 'exclude', 'ignore')),
                priority INTEGER DEFAULT 10,
                enabled BOOLEAN DEFAULT 1,
                category TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 爬虫任务日志表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                city TEXT,
                user TEXT,
                status TEXT,
                total_collected INTEGER,
                total_saved INTEGER,
                brand_excluded INTEGER,
                invalid_phone INTEGER,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                error_message TEXT
            )
        ''')
        
        conn.commit()
        logger.info("数据库初始化完成")
    
    def generate_fingerprint(self, restaurant):
        """生成餐厅数据指纹（用于去重）"""
        key_data = {
            'name': restaurant.get('name', '').strip().lower(),
            'city': restaurant.get('city', '').strip(),
            'address': restaurant.get('address', '').strip().lower(),
            'phone': restaurant.get('phone', '').strip()
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def save_restaurant(self, restaurant):
        """保存单个餐厅数据"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 生成指纹
            fingerprint = self.generate_fingerprint(restaurant)
            
            cursor.execute('''
                INSERT OR REPLACE INTO restaurants 
                (id, name, city, district, business_district, category, 
                 phone, rating, monthly_sales, address, fingerprint, 
                 phone_valid, is_brand, brand_name, updated_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                restaurant.get('id'),
                restaurant.get('name'),
                restaurant.get('city'),
                restaurant.get('district'),
                restaurant.get('business_district'),
                restaurant.get('category'),
                restaurant.get('phone'),
                restaurant.get('rating', 0),
                restaurant.get('monthly_sales', 0),
                restaurant.get('address'),
                fingerprint,
                restaurant.get('phone_valid', False),
                restaurant.get('is_brand', False),
                restaurant.get('brand_name', '')
            ))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"保存餐厅数据失败: {e}")
            return False
    
    def batch_save_restaurants(self, restaurants):
        """批量保存餐厅数据"""
        if not restaurants:
            return 0
        
        success_count = 0
        conn = self.get_connection()
        cursor = conn.cursor()
        
        for restaurant in restaurants:
            try:
                # 生成指纹
                fingerprint = self.generate_fingerprint(restaurant)
                
                cursor.execute('''
                    INSERT OR REPLACE INTO restaurants 
                    (id, name, city, district, business_district, category, 
                     phone, rating, monthly_sales, address, fingerprint, 
                     phone_valid, is_brand, brand_name, updated_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    restaurant.get('id'),
                    restaurant.get('name'),
                    restaurant.get('city'),
                    restaurant.get('district'),
                    restaurant.get('business_district'),
                    restaurant.get('category'),
                    restaurant.get('phone'),
                    restaurant.get('rating', 0),
                    restaurant.get('monthly_sales', 0),
                    restaurant.get('address'),
                    fingerprint,
                    restaurant.get('phone_valid', False),
                    restaurant.get('is_brand', False),
                    restaurant.get('brand_name', '')
                ))
                success_count += 1
            except Exception as e:
                logger.error(f"批量保存失败: {e}")
        
        conn.commit()
        return success_count
    
    def get_all_data(self, filters=None, page=1, per_page=50):
        """获取数据（带分页）"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 构建查询条件
            where_clauses = []
            params = []
            
            if filters:
                if filters.get('city'):
                    where_clauses.append("city = ?")
                    params.append(filters['city'])
                
                if filters.get('min_rating'):
                    where_clauses.append("rating >= ?")
                    params.append(float(filters['min_rating']))
                
                if filters.get('keyword'):
                    keyword = f"%{filters['keyword']}%"
                    where_clauses.append("(name LIKE ? OR address LIKE ? OR category LIKE ?)")
                    params.extend([keyword, keyword, keyword])
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            # 获取总数
            cursor.execute(f"SELECT COUNT(*) FROM restaurants WHERE {where_sql}", params)
            total = cursor.fetchone()[0]
            
            # 获取分页数据
            offset = (page - 1) * per_page
            query_sql = f"""
                SELECT * FROM restaurants 
                WHERE {where_sql}
                ORDER BY crawl_time DESC
                LIMIT ? OFFSET ?
            """
            
            cursor.execute(query_sql, params + [per_page, offset])
            rows = cursor.fetchall()
            
            data = [dict(row) for row in rows]
            
            return {
                'data': data,
                'total': total,
                'page': page,
                'per_page': per_page,
                'total_pages': (total + per_page - 1) // per_page
            }
        except Exception as e:
            logger.error(f"查询数据失败: {e}")
            return {'data': [], 'total': 0, 'page': page, 'per_page': per_page, 'total_pages': 0}
    
    def get_statistics(self):
        """获取统计信息"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            stats = {}
            
            cursor.execute("SELECT COUNT(*) FROM restaurants")
            stats['total'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT AVG(rating) FROM restaurants WHERE rating > 0")
            stats['avg_rating'] = round(cursor.fetchone()[0] or 0, 2)
            
            cursor.execute("SELECT AVG(monthly_sales) FROM restaurants WHERE monthly_sales > 0")
            stats['avg_sales'] = int(cursor.fetchone()[0] or 0)
            
            cursor.execute("SELECT COUNT(DISTINCT city) FROM restaurants")
            stats['city_count'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM restaurants WHERE phone_valid = 1")
            stats['valid_phone'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM restaurants WHERE is_brand = 1")
            stats['brand_count'] = cursor.fetchone()[0]
            
            return stats
        except Exception as e:
            logger.error(f"获取统计失败: {e}")
            return {}
    
    def cleanup_old_data(self, days=30):
        """清理旧数据"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                DELETE FROM restaurants 
                WHERE crawl_time < datetime('now', ?)
            ''', (f'-{days} days',))
            
            deleted = cursor.rowcount
            conn.commit()
            
            logger.info(f"清理了 {deleted} 条 {days} 天前的数据")
            return deleted
        except Exception as e:
            logger.error(f"清理数据失败: {e}")
            return 0