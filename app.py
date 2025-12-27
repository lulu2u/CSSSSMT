# app.py - 美团外卖数据采集系统（增强版基础框架）
import os
import json
import logging
import threading
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 创建必要的目录
BASE_DIR = Path(__file__).parent
for dir_name in ['static', 'templates', 'database', 'logs', 'exports', 'config', 'uploads']:
    dir_path = BASE_DIR / dir_name
    dir_path.mkdir(exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(BASE_DIR / 'logs' / 'app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建Flask应用
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'development-secret-key')

# ==================== 用户认证 ====================
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

# ==================== 数据库初始化 ====================
try:
    from database import EnhancedDatabaseManager
    db = EnhancedDatabaseManager()
    logger.info("✅ 使用增强版数据库管理器")
except ImportError as e:
    logger.error(f"无法导入增强数据库: {e}")
    # 如果失败，使用一个简单的替代方案
    class SimpleDB:
        def __init__(self):
            self.data = []
            logger.warning("使用简易内存数据库（仅演示）")
        def get_statistics(self):
            return {'total': 0, 'avg_rating': 0, 'avg_sales': 0, 'city_count': 0}
        def get_all_data(self, filters=None):
            return []
        def save_restaurant(self, restaurant):
            return True
    db = SimpleDB()

# ==================== 爬虫管理器 ====================
class EnhancedCrawlerManager:
    def __init__(self):
        self.active_tasks = {}
        self.lock = threading.Lock()
    
    def start_crawl(self, city, user, max_results=100):
        """启动爬虫任务（基础版本，后续增强）"""
        task_id = datetime.now().strftime('%Y%m%d%H%M%S')
        
        thread = threading.Thread(
            target=self._demo_crawl_task,
            args=(task_id, city, user, max_results),
            daemon=True
        )
        
        with self.lock:
            self.active_tasks[task_id] = {
                'thread': thread,
                'city': city,
                'user': user,
                'max_results': max_results,
                'status': 'running',
                'progress': 0,
                'total': max_results,
                'success': 0,
                'message': f'正在采集{city}的数据...',
                'start_time': datetime.now().isoformat()
            }
        
        thread.start()
        return task_id
    
    def _demo_crawl_task(self, task_id, city, user, max_results):
        """演示爬虫任务（后续替换为真实爬虫）"""
        try:
            task = self.active_tasks.get(task_id)
            if not task:
                return
            
            # 模拟爬虫过程
            import time
            import random
            
            # 生成模拟数据
            names = ['肯德基', '麦当劳', '海底捞', '星巴克', '必胜客', '真功夫', '永和大王']
            districts = ['朝阳区', '海淀区', '东城区', '西城区', '丰台区']
            categories = ['快餐简餐', '火锅', '咖啡', '西餐', '中餐']
            
            for i in range(max_results):
                if task['status'] != 'running':
                    break
                
                # 模拟爬取过程
                time.sleep(0.05)  # 模拟网络延迟
                
                # 生成模拟餐厅数据
                restaurant = {
                    'id': f"{city[:2]}{task_id[-6:]}{i:04d}",
                    'name': f"{random.choice(names)}（{random.choice(districts)}店）",
                    'city': city,
                    'district': random.choice(districts),
                    'business_district': f"{random.choice(districts)}商圈",
                    'category': random.choice(categories),
                    'phone': f"1{random.randint(30, 39)}{random.randint(10000000, 99999999):08d}",
                    'rating': round(random.uniform(3.5, 5.0), 1),
                    'monthly_sales': random.randint(100, 20000),
                    'address': f"{random.choice(districts)}路{random.randint(1, 999)}号",
                    'fingerprint': f"demo_{city}_{i}",
                    'phone_valid': True,
                    'is_brand': random.choice([True, False]),
                    'brand_name': random.choice(['', '肯德基', '麦当劳'])
                }
                
                # 保存到数据库
                if hasattr(db, 'save_restaurant'):
                    db.save_restaurant(restaurant)
                    task['success'] += 1
                
                # 更新进度
                task['progress'] = int((i + 1) / max_results * 100)
                task['last_update'] = datetime.now().isoformat()
            
            task['status'] = 'completed'
            task['message'] = f'采集完成！共收集{task["success"]}条数据'
            logger.info(f"爬虫任务完成: {task_id}")
            
        except Exception as e:
            logger.error(f"爬虫任务失败: {e}")
            if task_id in self.active_tasks:
                self.active_tasks[task_id]['status'] = 'failed'
                self.active_tasks[task_id]['message'] = f'采集失败: {str(e)}'
    
    def get_task_status(self, task_id):
        """获取任务状态"""
        return self.active_tasks.get(task_id, {'error': '任务不存在'})
    
    def stop_crawl(self, task_id):
        """停止任务"""
        with self.lock:
            if task_id in self.active_tasks:
                self.active_tasks[task_id]['status'] = 'stopped'
                self.active_tasks[task_id]['message'] = '任务已停止'
                return True
        return False

crawler = EnhancedCrawlerManager()

# ==================== 路由定义 ====================

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
    data_result = db.get_all_data(filters)
    
    # 兼容不同格式的返回
    if isinstance(data_result, dict) and 'data' in data_result:
        data = data_result['data']
    else:
        data = data_result
    
    return render_template('data.html', data=data)

# ==================== API接口 ====================

@app.route('/api/crawl/start', methods=['POST'])
@require_login
def start_crawl_api():
    try:
        data = request.json
        city = data.get('city', '北京')
        max_results = int(data.get('max_results', 100))
        
        task_id = crawler.start_crawl(city, session.get('username'), max_results)
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '爬虫任务已启动'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/crawl/status/<task_id>')
@require_login
def get_crawl_status(task_id):
    status = crawler.get_task_status(task_id)
    return jsonify(status)

@app.route('/api/crawl/stop/<task_id>', methods=['POST'])
@require_login
def stop_crawl_api(task_id):
    success = crawler.stop_crawl(task_id)
    return jsonify({'success': success, 'message': '任务已停止' if success else '任务不存在'})

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
        import pandas as pd
        from datetime import datetime
        
        filters = request.json.get('filters', {})
        data_result = db.get_all_data(filters)
        
        # 兼容不同格式
        if isinstance(data_result, dict) and 'data' in data_result:
            data = data_result['data']
        else:
            data = data_result
        
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
    return jsonify({
        'status': 'healthy',
        'service': 'meituan-crawler-web',
        'version': 'enhanced-v1.0',
        'database': 'connected' if hasattr(db, 'save_restaurant') else 'simple',
        'timestamp': datetime.now().isoformat()
    })

# ==================== 新增功能路由（占位） ====================

@app.route('/brands')
@require_login
def brands_page():
    """品牌管理页面（后续实现）"""
    return render_template('brands.html', username=session.get('username'))

@app.route('/settings')
@require_login
def settings_page():
    """系统设置页面（后续实现）"""
    return render_template('settings.html', username=session.get('username'))

# ==================== 启动应用 ====================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    print("=" * 50)
    print("美团外卖数据采集系统 - 增强版")
    print("=" * 50)
    print(f"版本: 增强基础框架 v1.0")
    print(f"数据库: {'增强版' if hasattr(db, 'save_restaurant') else '简易版'}")
    print(f"访问地址: http://localhost:{port}")
    print(f"管理员: admin / admin123")
    print(f"普通用户: user / user123")
    print("=" * 50)
    
    # 测试数据库连接
    try:
        stats = db.get_statistics()
        print(f"数据库状态: 正常，现有数据: {stats.get('total', 0)} 条")
    except Exception as e:
        print(f"数据库警告: {e}")
    
    app.run(host='0.0.0.0', port=port, debug=os.environ.get('DEBUG', 'false').lower() == 'true')