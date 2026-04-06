# coding=utf-8
import logging
from uliweb import expose, json as json_func, request

logger = logging.getLogger('uliweb')


# Use a class to avoid global declaration issues
class UserStore:
    USERS = [
        {'id': 1, 'name': 'Zhang San', 'email': 'zhangsan@example.com'},
        {'id': 2, 'name': 'Li Si', 'email': 'lisi@example.com'},
        {'id': 3, 'name': 'Wang Wu', 'email': 'wangwu@example.com'},
    ]

    @classmethod
    def get_all(cls):
        return cls.USERS

    @classmethod
    def add(cls, user):
        cls.USERS.append(user)

    @classmethod
    def remove(cls, user_id):
        cls.USERS[:] = [u for u in cls.USERS if u['id'] != user_id]


# 首页 - 直接返回字典，框架会自动渲染模板
# 模板文件名为 index.html（在 templates 目录下）
@expose('/')
def index():
    """首页视图 - 直接返回字典，框架自动渲染模板"""
    users = UserStore.get_all()
    return {'users': users}


# 关于页面 - 直接返回字典
@expose('/about')
def about():
    """关于页面 - 直接返回字典，框架自动渲染模板"""
    return {}


# 用户列表页面 - 直接返回字典
@expose('/users')
def users():
    """用户列表页面 - 直接返回字典，框架自动渲染模板"""
    users = UserStore.get_all()
    return {'users': users}


# 用户列表 API - JSON 响应
@expose('/api/users')
async def users_list():
    """用户列表 API - JSON 响应"""
    if request.method == 'GET':
        return json_func({
            'success': True,
            'data': UserStore.get_all(),
            'total': len(UserStore.get_all())
        })
    elif request.method == 'POST':
        post_data = await request.get_POST()
        new_user = {
            'id': len(UserStore.get_all()) + 1,
            'name': post_data.get('name', ''),
            'email': post_data.get('email', '')
        }
        UserStore.add(new_user)
        return json_func({
            'success': True,
            'message': 'User created',
            'data': new_user
        })


@expose('/api/users/<int:user_id>')
def user_detail(user_id):
    """单个用户详情 API - JSON 响应"""
    users = UserStore.get_all()
    user = next((u for u in users if u['id'] == user_id), None)

    if not user:
        return json_func({
            'success': False,
            'error': 'User not found'
        }, status=404)

    if request.method == 'GET':
        return json_func({
            'success': True,
            'data': user
        })
    elif request.method == 'DELETE':
        UserStore.remove(user_id)
        return json_func({
            'success': True,
            'message': 'User deleted'
        })


@expose('/api/data', methods=['GET', 'POST'])
async def api_data():
    """测试 API - JSON 响应"""
    if request.method == 'POST':
        try:
            json_data = await request.get_json()
        except Exception:
            json_data = {}

        return json_func({
            'success': True,
            'message': 'JSON data received',
            'received_data': json_data
        })
    else:
        return json_func({
            'success': True,
            'message': 'GET request JSON response'
        })


@expose('/api/echo')
async def api_echo():
    """回显 API - 返回请求信息"""
    get_params = dict(request.query_params)

    try:
        post_data = await request.get_POST()
        post_params = dict(post_data) if post_data else {}
    except Exception:
        post_params = {}

    try:
        json_data = await request.get_json()
    except Exception:
        json_data = {}

    return json_func({
        'success': True,
        'method': request.method,
        'path': request.path,
        'query_params': get_params,
        'post_params': post_params,
        'json_data': json_data
    })


# WebSocket 测试页面 - 直接返回字典
@expose('/websocket-test')
def websocket_test():
    """WebSocket 测试页面 - 直接返回字典"""
    return {}


@expose('/ws/echo', websocket=True)
async def websocket_echo(websocket):
    """WebSocket Echo 服务示例"""
    # 添加调试日志
    logger.warning("[WebSocket] websocket_echo called!")
    logger.warning(f"[WebSocket] websocket.scope = {websocket.scope}")
    logger.warning(f"[WebSocket] websocket.url = {websocket.url}")
    logger.warning(f"[WebSocket] websocket.client = {websocket.client}")

    # 接受 WebSocket 连接
    try:
        await websocket.accept()
        logger.warning("[WebSocket] Connection accepted!")
    except Exception as e:
        logger.warning(f"[WebSocket] Accept failed: {e}")
        raise

    try:
        # 持续接收消息
        while True:
            # 接收客户端消息
            try:
                data = await websocket.receive_text()
                logger.warning(f"[WebSocket] Received: {data}")
            except Exception as e:
                logger.warning(f"[WebSocket] Receive error: {e}")
                raise

            # 发送回显消息
            try:
                await websocket.send_text(f"Echo: {data}")
                logger.warning(f"[WebSocket] Sent: Echo: {data}")
            except Exception as e:
                logger.warning(f"[WebSocket] Send error: {e}")
                raise

    except Exception as e:
        # 处理连接关闭
        logger.warning(f"[WebSocket] Connection closed, exception: {e}")
        pass


@expose('/ws/chat', websocket=True)
async def websocket_chat(websocket):
    """WebSocket 聊天服务示例"""
    # 接受 WebSocket 连接
    await websocket.accept()

    # 获取客户端信息
    client_host = websocket.client.host if hasattr(websocket, 'client') else 'unknown'

    try:
        # 发送欢迎消息
        await websocket.send_text(f"Welcome to chat! Your IP: {client_host}")

        # 持续接收消息
        message_count = 0
        while True:
            # 接收客户端消息
            data = await websocket.receive_text()
            message_count += 1

            # 处理消息
            if data.lower() in ['quit', 'exit', 'bye']:
                await websocket.send_text("Goodbye!")
                break
            elif data.lower() == 'count':
                await websocket.send_text(f"Messages received: {message_count}")
            else:
                # 发送聊天响应
                await websocket.send_text(f"You said: {data} (Message #{message_count})")

    except Exception as e:
        # 处理连接关闭
        await websocket.send_text(f"Connection closed: {str(e)}")
