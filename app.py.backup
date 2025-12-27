import os
import json
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for
import sqlite3
import pandas as pd
import threading
import time
import random
from pathlib import Path

# 创建必要的目录
BASE_DIR = Path(__file__).parent
for dir_name in ['static', 'templates', 'database', 'logs', 'exports']:
    dir_path = BASE_DIR / dir_name
    dir_path.mkdir(exist_ok=True)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建Flask应用
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'meituan-web-secret-2024')

# 用户认证
USERS = {
    'admin': {'password': 'admin123', 'role': 'admin'},
    'user': {'password': 'user123', 'role': 'user'}
}

def require_login(f):
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# 数据库管理器
class DatabaseManager:
    def __init__(self):
        self.db_path = BASE_DIR / 'database' / 'meituan.db'
        self.init_database()
    
    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS restaurants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT,
                district TEXT,
                business_district TEXT,
                category TEXT,
                phone TEXT,
                rating REAL,
                monthly_sales INTEGER,
                address TEXT,
                crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
    
    def save_restaurant(self, restaurant):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO restaurants 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                restaurant['id'], restaurant['name'], restaurant['city'],
                restaurant['district'], restaurant['business_district'],
                restaurant['category'], restaurant['phone'], restaurant['rating'],
                restaurant['monthly_sales'], restaurant['address']
            ))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"保存数据失败: {e}")
            return False
    
    def get_all_data(self, filters=None):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = "SELECT * FROM restaurants WHERE 1=1"
            params = []
            
            if filters and filters.get('city'):
                query += " AND city = ?"
                params.append(filters['city'])
            
            query += " ORDER BY crawl_time DESC LIMIT 1000"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            data = [dict(row) for row in rows]
            conn.close()
            return data
        except Exception as e:
            logger.error(f"查询数据失败: {e}")
            return []
    
    def get_statistics(self):
        try:
            conn = sqlite3.connect(self.db_path)
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
            
            conn.close()
            return stats
        except Exception as e:
            logger.error(f"获取统计失败: {e}")
            return {}

db = DatabaseManager()

# 爬虫管理器
class CrawlerManager:
    def __init__(self):
        self.active_tasks = {}
    
    def start_crawl(self, city, user):
        task_id = datetime.now().strftime('%Y%m%d%H%M%S')
        thread = threading.Thread(target=self._crawl_task, args=(task_id, city, user), daemon=True)
        self.active_tasks[task_id] = {
            'thread': thread, 'city': city, 'user': user,
            'status': 'running', 'progress': 0, 'total': 50, 'success': 0
        }
        thread.start()
        return task_id
    
    def _crawl_task(self, task_id, city, user):
        try:
            task = self.active_tasks[task_id]
            sample_data = self._generate_sample_data(city)
            
            for i, restaurant in enumerate(sample_data):
                if task['status'] != 'running':
                    break
                time.sleep(0.05)
                if db.save_restaurant(restaurant):
                    task['success'] += 1
                task['progress'] = int((i + 1) / len(sample_data) * 100)
            
            task['status'] = 'completed'
            logger.info(f"爬虫任务完成: {task_id}")
        except Exception as e:
            logger.error(f"爬虫任务失败: {e}")
            if task_id in self.active_tasks:
                self.active_tasks[task_id]['status'] = 'failed'
    
    def _generate_sample_data(self, city):
        restaurants = []
        names = ['肯德基', '麦当劳', '海底捞', '星巴克', '必胜客', '真功夫', '永和大王']
        districts = ['朝阳区', '海淀区', '东城区', '西城区', '丰台区']
        categories = ['快餐简餐', '火锅', '咖啡', '西餐', '中餐']
        
        for i in range(50):
            restaurant = {
                'id': f"{city[:2]}{datetime.now().strftime('%H%M%S')}{i}",
                'name': f"{random.choice(names)}（{random.choice(districts)}店）",
                'city': city,
                'district': random.choice(districts),
                'business_district': f"{random.choice(districts)}商圈",
                'category': random.choice(categories),
                'phone': f"1{random.randint(30, 39)}{random.randint(10000000, 99999999):08d}",
                'rating': round(random.uniform(3.5, 5.0), 1),
                'monthly_sales': random.randint(100, 20000),
                'address': f"{random.choice(districts)}路{random.randint(1, 999)}号"
            }
            restaurants.append(restaurant)
        
        return restaurants
    
    def get_task_status(self, task_id):
        return self.active_tasks.get(task_id, {'error': '任务不存在'})

crawler = CrawlerManager()

# 路由定义
@app.route('/')
def index():
    if 'username' not in session:
        return redirect(url_for('login'))
    stats = db.get_statistics()
    return render_template('index.html', stats=stats, username=session.get('username'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if username in USERS and USERS[username]['password'] == password:
            session['username'] = username
            session['role'] = USERS[username]['role']
            return redirect(url_for('index'))
        return render_template('login.html', error='用户名或密码错误')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/crawl')
@require_login
def crawl_page():
    return render_template('crawl.html')

@app.route('/data')
@require_login
def data_page():
    city = request.args.get('city', '')
    filters = {'city': city} if city else {}
    data = db.get_all_data(filters)
    return render_template('data.html', data=data)

# API接口
@app.route('/api/crawl/start', methods=['POST'])
@require_login
def start_crawl_api():
    data = request.json
    city = data.get('city', '北京')
    task_id = crawler.start_crawl(city, session.get('username'))
    return jsonify({'success': True, 'task_id': task_id, 'message': '爬虫任务已启动'})

@app.route('/api/crawl/status/<task_id>')
@require_login
def get_crawl_status(task_id):
    status = crawler.get_task_status(task_id)
    return jsonify(status)

@app.route('/api/data')
@require_login
def get_data_api():
    city = request.args.get('city', '')
    filters = {'city': city} if city else {}
    data = db.get_all_data(filters)
    return jsonify({'success': True, 'data': data})

@app.route('/api/export', methods=['POST'])
@require_login
def export_data():
    try:
        filters = request.json.get('filters', {})
        data = db.get_all_data(filters)
        
        if not data:
            return jsonify({'error': '没有数据可导出'}), 400
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = BASE_DIR / 'exports'
        export_dir.mkdir(exist_ok=True)
        
        filename = f"meituan_data_{timestamp}.xlsx"
        filepath = export_dir / filename
        
        df = pd.DataFrame(data)
        df.to_excel(filepath, index=False)
        
        return jsonify({
            'success': True,
            'filename': filename,
            'message': '导出成功'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>')
@require_login
def download_file(filename):
    filepath = BASE_DIR / 'exports' / filename
    if filepath.exists():
        return send_file(filepath, as_attachment=True)
    return jsonify({'error': '文件不存在'}), 404

@app.route('/health')
def health_check():
    return jsonify({'status': 'healthy', 'service': 'meituan-crawler-web'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("美团外卖数据采集系统启动")
    print(f"访问地址: http://localhost:{port}")
    print("管理员: admin / admin123")
    print("普通用户: user / user123")
    app.run(host='0.0.0.0', port=port)