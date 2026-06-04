# coding=utf-8
"""
Chatbot - LLM 聊天界面
支持配置 OpenAI 兼容 API (OpenAI, Ollama, LocalAI 等)
使用 WebSocket 进行流式输出
"""
import json
import logging
import re
import aiohttp
from uliweb import expose, json as json_func, request, settings
from starlette.responses import Response as StarletteResponse

logger = logging.getLogger('chatbot')

# 用于匹配 think 标签的正则表达式
THINK_START_PATTERN = re.compile(r'<think>')
THINK_END_PATTERN = re.compile(r'</think>')


# ============= 配置相关 =============

def get_api_config():
    """获取 API 配置"""
    return {
        'base_url': settings.CHATBOT.openai_base_url,
        'api_key': settings.CHATBOT.openai_api_key,
        'model': settings.CHATBOT.default_model,
        'verify_ssl': settings.CHATBOT.verify_ssl,
    }



async def call_openai_api_stream(messages: list, model: str = None):
    """
    流式调用 OpenAI 兼容 API

    Args:
        messages: 消息列表
        model: 模型名称

    Yields:
        API 响应块
    """
    config = get_api_config()

    base_url = config['base_url'].rstrip('/')
    api_key = config['api_key']
    model = model or config['model']

    # 构建请求 URL
    # 如果 base_url 已经包含 /v1 后缀，不再添加 /v1
    if base_url.endswith('/v1'):
        url = f"{base_url}/chat/completions"
    else:
        url = f"{base_url}/v1/chat/completions"

    headers = {
        'Content-Type': 'application/json',
    }

    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'

    payload = {
        'model': model,
        'messages': messages,
        'stream': True,
    }

    # 创建 SSL connector
    # verify_ssl 为 False 时禁用 SSL 验证
    ssl = False if not config['verify_ssl'] else None

    async with aiohttp.ClientSession(
        trust_env=True,
        connector=aiohttp.TCPConnector(ssl=ssl)
    ) as session:
        # 流式请求
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"API error: {response.status}, {error_text}")
                raise Exception(f"API error: {response.status}")

            async for line in response.content:
                line = line.decode('utf-8').strip()
                logger.debug(f"Raw line: {line[:200]}...")
                if line.startswith('data: '):
                    data = line[6:]
                    if data == '[DONE]':
                        logger.info("Received [DONE], stream finished")
                        break
                    try:
                        # 注意：这里需要重新导入 json 模块，因为运行时 json 可能被替换为 uliweb 的 json 函数
                        import json as json_module
                        chunk_data = json_module.loads(data)
                        yield chunk_data
                    except json_module.JSONDecodeError as e:
                        logger.warning(f"JSON decode error: {e}, line: {line[:100]}...")
                        continue


async def call_openai_api(messages: list, stream: bool = False, model: str = None):
    """
    调用 OpenAI 兼容 API

    Args:
        messages: 消息列表
        stream: 是否流式输出
        model: 模型名称

    Returns:
        响应内容（完整响应）
    """
    config = get_api_config()

    base_url = config['base_url'].rstrip('/')
    api_key = config['api_key']
    model = model or config['model']

    # 构建请求 URL
    # 如果 base_url 已经包含 /v1 后缀，不再添加 /v1
    if base_url.endswith('/v1'):
        url = f"{base_url}/chat/completions"
    else:
        url = f"{base_url}/v1/chat/completions"

    headers = {
        'Content-Type': 'application/json',
    }

    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'

    payload = {
        'model': model,
        'messages': messages,
        'stream': stream,
    }

    logger.info(f"Calling API: {url}, model: {model}, stream: {stream}, verify_ssl: {config['verify_ssl']}")

    # 创建 SSL connector
    # verify_ssl 为 False 时禁用 SSL 验证
    ssl = False if not config['verify_ssl'] else None

    async with aiohttp.ClientSession(
        trust_env=True,
        connector=aiohttp.TCPConnector(ssl=ssl)
    ) as session:
        # 非流式请求
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"API error: {response.status}, {error_text}")
                raise Exception(f"API error: {response.status}")

            return await response.json()


@expose('/')
def index():
    """首页 - Chatbot 聊天界面"""
    config = get_api_config()
    # 提取显示用的 URL（去除 /v1 后缀）
    display_url = config['base_url'].replace('/v1', '').replace('https://', '').replace('http://', '')
    return {
        'base_url': config['base_url'],
        'model': config['model'],
        'display_url': display_url,
    }


@expose('/chat')
def chat_page():
    """专门的聊天页面"""
    return {}


# ============= SSE 聊天接口 =============

async def sse_stream_generator(message: str):
    """
    SSE 流生成器

    Args:
        message: 用户消息

    Yields:
        SSE 格式的数据块
    """
    # 注意：这里需要重新导入 json 模块，因为运行时 json 可能被替换为 uliweb 的 json 函数
    import json as json_module

    # 获取配置
    config = get_api_config()
    logger.info(f"SSE stream: config model={config['model']}, base_url={config['base_url']}")

    # 消息历史（用于上下文）
    messages = []

    logger.info(f"SSE stream: received message='{message[:100]}...' (length={len(message)})")

    # 特殊消息：__WELCOME__ 仅用于获取欢迎消息
    if message == '__WELCOME__':
        logger.info("SSE stream: special welcome message, sending welcome only")
        # 发送欢迎消息
        welcome = {
            'type': 'welcome',
            'message': f'欢迎连接到 Chatbot！当前模型: {config["model"]}',
            'model': config['model'],
            'base_url': config['base_url'],
        }
        welcome_data = f"data: {json_module.dumps(welcome, ensure_ascii=False)}\n\n"
        logger.info(f"SSE stream: sending welcome, length={len(welcome_data)}")
        yield welcome_data
        return

    # 普通消息：先发送欢迎消息
    welcome = {
        'type': 'welcome',
        'message': f'欢迎连接到 Chatbot！当前模型: {config["model"]}',
        'model': config['model'],
        'base_url': config['base_url'],
    }
    welcome_data = f"data: {json_module.dumps(welcome, ensure_ascii=False)}\n\n"
    logger.info(f"SSE stream: sending welcome, length={len(welcome_data)}")
    yield welcome_data

    if not message:
        error_msg = {
            'type': 'error',
            'message': '消息内容不能为空'
        }
        error_data = f"data: {json_module.dumps(error_msg, ensure_ascii=False)}\n\n"
        logger.info(f"SSE stream: sending error (empty message), length={len(error_data)}")
        yield error_data
        return

    # 添加用户消息到历史
    messages.append({'role': 'user', 'content': message})

    # 发送"正在思考"状态
    status_msg = {
        'type': 'status',
        'message': '正在生成响应...'
    }
    status_data = f"data: {json_module.dumps(status_msg, ensure_ascii=False)}\n\n"
    logger.info(f"SSE stream: sending status, length={len(status_data)}")
    yield status_data

    try:
        logger.info("SSE stream: starting API call...")
        # 调用 API 获取响应
        full_response = ""
        thinking_content = ""
        in_thinking = False
        chunk_count = 0

        async for chunk in call_openai_api_stream(messages, model=config['model']):
            chunk_count += 1
            choices = chunk.get('choices')
            if not choices:
                logger.debug(f"SSE stream: chunk {chunk_count} has no choices: {chunk}")
                continue
            delta = choices[0].get('delta', {})
            content_chunk = delta.get('content', '') or ''
            if not content_chunk:
                continue
            logger.debug(f"SSE stream: chunk {chunk_count}, content_length={len(content_chunk)}, content='{content_chunk[:50]}...'")

            if content_chunk:
                # 处理可能存在的 think 标签
                remaining = content_chunk
                all_start_matches = list(THINK_START_PATTERN.finditer(remaining))
                all_end_matches = list(THINK_END_PATTERN.finditer(remaining))
                loop_count = 0
                max_loops = 50

                while remaining and loop_count < max_loops:
                    loop_count += 1

                    if in_thinking:
                        match_end = THINK_END_PATTERN.search(remaining)
                        if match_end:
                            think_end = match_end.start()
                            think_text = remaining[:think_end]
                            if think_text.strip():
                                thinking_content += think_text.strip()
                                think_data = f"data: {json_module.dumps({'type': 'think', 'content': think_text.strip()}, ensure_ascii=False)}\n\n"
                                logger.debug(f"SSE stream: sending think chunk, length={len(think_data)}")
                                yield think_data
                            remaining = remaining[match_end.end():]
                            think_done_data = f"data: {json_module.dumps({'type': 'think_done'}, ensure_ascii=False)}\n\n"
                            logger.debug(f"SSE stream: sending think_done, length={len(think_done_data)}")
                            yield think_done_data
                            thinking_content = ""
                            in_thinking = False
                            if remaining:
                                continue
                            break
                        else:
                            if remaining.strip():
                                thinking_content += remaining.strip()
                                think_data = f"data: {json_module.dumps({'type': 'think', 'content': remaining.strip()}, ensure_ascii=False)}\n\n"
                                logger.debug(f"SSE stream: sending think (continuing), length={len(think_data)}")
                                yield think_data
                            break
                    else:
                        match_start = THINK_START_PATTERN.search(remaining)
                        if match_start:
                            think_start = match_start.start()
                            if think_start > 0:
                                chunk_text = remaining[:think_start]
                                if chunk_text.strip():
                                    full_response += chunk_text.strip()
                                    chunk_data = f"data: {json_module.dumps({'type': 'chunk', 'content': chunk_text.strip()}, ensure_ascii=False)}\n\n"
                                    logger.debug(f"SSE stream: sending chunk (before think), length={len(chunk_data)}")
                                    yield chunk_data
                            remaining = remaining[match_start.end():]
                            in_thinking = True
                            continue
                        else:
                            if remaining.strip():
                                full_response += remaining.strip()
                                chunk_data = f"data: {json_module.dumps({'type': 'chunk', 'content': remaining.strip()}, ensure_ascii=False)}\n\n"
                                logger.debug(f"SSE stream: sending chunk, length={len(chunk_data)}")
                                yield chunk_data
                            break

        logger.info(f"SSE stream: API call finished, total_chunks={chunk_count}, full_response_length={len(full_response)}")

        # 发送完成信号
        if full_response:
            done_data = f"data: {json_module.dumps({'type': 'message_done', 'role': 'assistant'}, ensure_ascii=False)}\n\n"
            logger.info(f"SSE stream: sending message_done, length={len(done_data)}")
            yield done_data
            messages.append({'role': 'assistant', 'content': full_response})
        else:
            error_data = f"data: {json_module.dumps({'type': 'error', 'message': '未收到有效响应'}, ensure_ascii=False)}\n\n"
            logger.warning(f"SSE stream: no content received, sending error, length={len(error_data)}")
            yield error_data

    except Exception as e:
        logger.exception(f"SSE stream API call error: {e}")
        error_data = f"data: {json_module.dumps({'type': 'error', 'message': f'API 调用错误: {str(e)}'}, ensure_ascii=False)}\n\n"
        logger.info(f"SSE stream: sending error, length={len(error_data)}")
        yield error_data


@expose('/sse/chat')
async def sse_chat():
    """
    SSE (Server-Sent Events) 聊天接口

    使用 GET 请求，消息通过查询参数传递
    使用 ASGI 风格的流式响应
    """
    logger.info("=== SSE Chat START ===")

    # 从查询参数获取消息
    message = request.params.get('message', '').strip()
    logger.info(f"SSE: received message='{message[:100]}...' (length={len(message)})")

    # 创建 SSEStreamResponse
    sse_response = SSEStreamResponse(message)
    logger.info(f"SSE: SSEStreamResponse created, media_type={sse_response.media_type}")

    # 返回 ASGI 风格的流式响应
    return sse_response


@expose('/sse/chat', methods=['POST'])
async def sse_chat_post():
    """
    SSE (Server-Sent Events) 聊天接口 - POST 版本

    支持通过 POST 请求发送消息，适合更长的内容
    """
    from uliweb import response

    # 设置 SSE 响应头
    response.content_type = 'text/event-stream'
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['X-Accel-Buffering'] = 'no'

    # 获取配置
    config = get_api_config()

    # 发送欢迎消息
    welcome = {
        'type': 'welcome',
        'message': f'欢迎连接到 Chatbot！当前模型: {config["model"]}',
        'model': config['model'],
        'base_url': config['base_url'],
    }
    yield f"data: {json.dumps(welcome, ensure_ascii=False)}\n\n"

    # 消息历史（用于上下文）
    messages = []

    try:
        json_data = await request.get_json()
    except Exception:
        json_data = {}

    message = json_data.get('message', '').strip()

    if not message:
        error_msg = {
            'type': 'error',
            'message': '消息内容不能为空'
        }
        yield f"data: {json.dumps(error_msg, ensure_ascii=False)}\n\n"
        return

    # 添加用户消息到历史
    messages.append({'role': 'user', 'content': message})

    # 发送"正在思考"状态
    status_msg = {
        'type': 'status',
        'message': '正在生成响应...'
    }
    yield f"data: {json.dumps(status_msg, ensure_ascii=False)}\n\n"

    try:
        # 调用 API 获取响应
        full_response = ""
        thinking_content = ""
        in_thinking = False

        async for chunk in call_openai_api_stream(messages, model=config['model']):
            choices = chunk.get('choices')
            if not choices:
                continue
            delta = choices[0].get('delta', {})
            content_chunk = delta.get('content', '')

            if content_chunk:
                # 处理可能存在的 think 标签
                remaining = content_chunk
                all_start_matches = list(THINK_START_PATTERN.finditer(remaining))
                all_end_matches = list(THINK_END_PATTERN.finditer(remaining))
                loop_count = 0
                max_loops = 50

                while remaining and loop_count < max_loops:
                    loop_count += 1

                    if in_thinking:
                        match_end = THINK_END_PATTERN.search(remaining)
                        if match_end:
                            think_end = match_end.start()
                            think_text = remaining[:think_end]
                            if think_text.strip():
                                thinking_content += think_text.strip()
                                yield f"data: {json.dumps({'type': 'think', 'content': think_text.strip()}, ensure_ascii=False)}\n\n"
                            remaining = remaining[match_end.end():]
                            yield f"data: {json.dumps({'type': 'think_done'}, ensure_ascii=False)}\n\n"
                            thinking_content = ""
                            in_thinking = False
                            if remaining:
                                continue
                            break
                        else:
                            if remaining.strip():
                                thinking_content += remaining.strip()
                                yield f"data: {json.dumps({'type': 'think', 'content': remaining.strip()}, ensure_ascii=False)}\n\n"
                            break
                    else:
                        match_start = THINK_START_PATTERN.search(remaining)
                        if match_start:
                            think_start = match_start.start()
                            if think_start > 0:
                                chunk_text = remaining[:think_start]
                                if chunk_text.strip():
                                    full_response += chunk_text.strip()
                                    yield f"data: {json.dumps({'type': 'chunk', 'content': chunk_text.strip()}, ensure_ascii=False)}\n\n"
                            remaining = remaining[match_start.end():]
                            in_thinking = True
                            continue
                        else:
                            if remaining.strip():
                                full_response += remaining.strip()
                                yield f"data: {json.dumps({'type': 'chunk', 'content': remaining.strip()}, ensure_ascii=False)}\n\n"
                            break

        # 发送完成信号
        if full_response:
            yield f"data: {json.dumps({'type': 'message_done', 'role': 'assistant'}, ensure_ascii=False)}\n\n"
            messages.append({'role': 'assistant', 'content': full_response})
        else:
            yield f"data: {json.dumps({'type': 'error', 'message': '未收到有效响应'}, ensure_ascii=False)}\n\n"

    except Exception as e:
        logger.exception(f"SSE API call error: {e}")
        yield f"data: {json.dumps({'type': 'error', 'message': f'API 调用错误: {str(e)}'}, ensure_ascii=False)}\n\n"


# ============= WebSocket 聊天接口 =============

@expose('/ws/chat', websocket=True)
async def websocket_chat(websocket):
    """
    WebSocket 聊天接口

    连接后可以发送消息，通过配置的 OpenAI API 获取响应
    """
    # 接受连接
    await websocket.accept()

    # 发送欢迎消息
    config = get_api_config()
    welcome = {
        'type': 'welcome',
        'message': f'欢迎连接到 Chatbot！当前模型: {config["model"]}',
        'model': config['model'],
        'base_url': config['base_url'],
    }
    await websocket.send_json(welcome)

    # 消息历史（用于上下文）
    messages = []

    try:
        while True:
            # 接收客户端消息
            try:
                data = await websocket.receive_text()
            except Exception as e:
                # 忽略连接关闭相关的错误，这些是正常的断开连接行为
                error_str = str(e)
                # 检查是否是连接关闭错误 (code 1005 表示没有收到关闭状态码)
                if '1005' in error_str or 'NO_STATUS_RCVD' in error_str or 'ConnectionClosed' in error_str:
                    # 连接正常关闭，不再记录为错误
                    logger.debug(f"WebSocket connection closed: {e}")
                else:
                    logger.error(f"WebSocket receive error: {e}")
                break

            # 解析消息
            # 注意：这里需要重新导入 json 模块，因为运行时 json 可能被替换为 uliweb 的 json 函数
            import json as json_module
            try:
                message_data = json_module.loads(data)
            except json_module.JSONDecodeError:
                # 如果不是 JSON，当作纯文本处理
                message_data = {'type': 'message', 'content': data}

            msg_type = message_data.get('type', 'message')

            if msg_type == 'ping':
                # 处理心跳
                await websocket.send_json({'type': 'pong'})
                continue

            if msg_type == 'clear':
                # 清除历史
                messages = []
                await websocket.send_json({
                    'type': 'system',
                    'message': '聊天历史已清除'
                })
                continue

            if msg_type == 'config':
                # 更新配置
                new_model = message_data.get('model')
                if new_model:
                    config['model'] = new_model
                await websocket.send_json({
                    'type': 'system',
                    'message': f'模型已切换为: {config["model"]}'
                })
                continue

            # 获取消息内容
            content = message_data.get('content', '')

            if not content:
                await websocket.send_json({
                    'type': 'error',
                    'message': '消息内容不能为空'
                })
                continue

            # 添加到历史
            messages.append({'role': 'user', 'content': content})

            # 发送"正在思考"状态
            await websocket.send_json({
                'type': 'status',
                'message': '正在生成响应...'
            })

            try:
                # 调用 API 获取响应
                full_response = ""
                thinking_content = ""
                in_thinking = False

                async for chunk in call_openai_api_stream(messages, model=config['model']):
                    choices = chunk.get('choices')
                    if not choices:
                        # API 返回的 chunk 可能没有 choices（如仅包含 finish_reason）
                        logger.debug(f"Skipping chunk without choices: {chunk}")
                        continue
                    delta = choices[0].get('delta', {})
                    content_chunk = delta.get('content', '')

                    if content_chunk:
                        # 详细调试日志
                        logger.info(f"=== Content chunk start ===")
                        logger.info(f"Raw content repr: {repr(content_chunk)}")
                        logger.info(f"Raw content str: {content_chunk}")
                        logger.info(f"Length: {len(content_chunk)}")

                        # 显示每个字符的 Unicode 码点 (最多100个字符)
                        char_info = []
                        for i, c in enumerate(content_chunk[:100]):
                            if c == '\n':
                                char_info.append(f'{i}:\\n(U+000A)')
                            elif c == '\r':
                                char_info.append(f'{i}:\\r(U+000D)')
                            elif c == '\t':
                                char_info.append(f'{i}:\\t(U+0009)')
                            else:
                                char_info.append(f'{i}:{repr(c)}(U+{ord(c):04X})')
                        logger.info(f"Chars detail: {' | '.join(char_info)}")

                        # 处理可能存在的 think 标签（可能在同一个 chunk 中有多个标签）
                        remaining = content_chunk

                        # 查找所有 think 标签的位置
                        all_start_matches = list(THINK_START_PATTERN.finditer(remaining))
                        all_end_matches = list(THINK_END_PATTERN.finditer(remaining))
                        logger.info(f"All 【 Matches: {[m.start() for m in all_start_matches]}")
                        logger.info(f"All 】 Matches: {[m.start() for m in all_end_matches]}")
                        loop_count = 0
                        max_loops = 50  # 防止无限循环

                        while remaining and loop_count < max_loops:
                            loop_count += 1
                            logger.info(f"--- Loop {loop_count}, in_thinking={in_thinking}, remaining={repr(remaining)[:200]} ---")

                            if in_thinking:
                                # 寻找最近的</think>标签 - 使用正则表达式匹配完整标签
                                match_end = THINK_END_PATTERN.search(remaining)
                                logger.info(f"Search for 】, match_end={match_end}")
                                if match_end:
                                    think_end = match_end.start()
                                    # 提取think内容（到</think>之前）
                                    think_text = remaining[:think_end]
                                    logger.info(f"Think content found at {think_end}: {repr(think_text)}")
                                    logger.info(f"Think text chars: {' | '.join([f'{repr(c)}(U+{ord(c):04X})' for c in think_text])}")
                                    if think_text.strip():
                                        thinking_content += think_text.strip()
                                        await websocket.send_json({
                                            'type': 'think',
                                            'content': think_text.strip(),
                                        })
                                    # 跳过</think>标签
                                    remaining = remaining[match_end.end():]
                                    logger.info(f"After think_end, remaining: {repr(remaining)[:200]}")
                                    # 发送思考完成信号
                                    await websocket.send_json({
                                        'type': 'think_done'
                                    })
                                    thinking_content = ""
                                    in_thinking = False
                                    if remaining:
                                        continue
                                    break
                                else:
                                    # 没有结束标签，累加think内容
                                    logger.info(f"Think continuing (no 】 found): {repr(remaining)[:200]}")
                                    if remaining.strip():
                                        thinking_content += remaining.strip()
                                        await websocket.send_json({
                                            'type': 'think',
                                            'content': remaining.strip(),
                                        })
                                    break
                            else:
                                # 寻找下一个<think>标签 - 使用正则表达式匹配完整标签
                                match_start = THINK_START_PATTERN.search(remaining)
                                logger.info(f"Search for 【, match_start={match_start}")
                                if match_start:
                                    think_start = match_start.start()
                                    logger.info(f"Think start found at {think_start}")
                                    # 发送think标签之前的正常内容
                                    if think_start > 0:
                                        chunk_text = remaining[:think_start]
                                        logger.info(f"Before think: {repr(chunk_text)}")
                                        if chunk_text.strip():
                                            full_response += chunk_text.strip()
                                            await websocket.send_json({
                                                'type': 'chunk',
                                                'content': chunk_text.strip(),
                                            })
                                    # 跳过<think>标签
                                    remaining = remaining[match_start.end():]
                                    logger.info(f"After think_start, remaining: {repr(remaining)[:200]}")
                                    in_thinking = True
                                    continue
                                else:
                                    # 没有think标签，作为正常内容处理
                                    logger.info(f"Normal chunk (no tag): {repr(remaining)[:200]}")
                                    if remaining.strip():
                                        full_response += remaining.strip()
                                        await websocket.send_json({
                                            'type': 'chunk',
                                            'content': remaining.strip(),
                                        })
                                    break
                        logger.info(f"=== Content chunk end (loops: {loop_count}) ===")

                # 发送完成信号（不再发送完整的 message，因为 chunk 已经包含了所有内容）
                if full_response:
                    await websocket.send_json({
                        'type': 'message_done',
                        'role': 'assistant',
                    })
                    # 添加助手消息到历史
                    messages.append({'role': 'assistant', 'content': full_response})
                else:
                    logger.warning("No content received from API")
                    await websocket.send_json({
                        'type': 'error',
                        'message': '未收到有效响应'
                    })

            except Exception as e:
                logger.exception(f"API call error: {e}")
                await websocket.send_json({
                    'type': 'error',
                    'message': f'API 调用错误: {str(e)}'
                })

            # 限制历史长度
            if len(messages) > 20:
                messages = messages[-20:]

    except Exception as e:
        import traceback
        logger.error(f"WebSocket error: {e}\n{traceback.format_exc()}")
        await websocket.send_json({
            'type': 'error',
            'message': f'连接错误: {str(e)}'
        })


# ============= HTTP API 代理接口 =============

@expose('/api/chat', methods=['POST'])
async def api_chat():
    """
    HTTP API 聊天接口
    支持流式输出
    """
    try:
        json_data = await request.get_json()
    except Exception:
        json_data = {}

    messages = json_data.get('messages', [])
    model = json_data.get('model')
    stream = json_data.get('stream', False)

    if not messages:
        return json_func({
            'error': {
                'message': 'No messages provided',
                'type': 'invalid_request_error',
            }
        }, status=400)

    try:
        if stream:
            return StreamingResponse(
                call_openai_api_stream(messages, model=model)
            )
        else:
            result = await call_openai_api(messages, stream=False, model=model)
            return json_func(result)
    except Exception as e:
        logger.error(f"API error: {e}")
        return json_func({
            'error': {
                'message': str(e),
                'type': 'api_error',
            }
        }, status=500)


@expose('/api/models')
async def api_models():
    """获取可用模型列表"""
    config = get_api_config()

    # 尝试从 API 获取模型列表
    base_url = config['base_url'].rstrip('/')
    api_key = config['api_key']

    # 构建请求 URL
    # 如果 base_url 已经包含 /v1 后缀，不再添加 /v1
    if base_url.endswith('/v1'):
        url = f"{base_url}/models"
    else:
        url = f"{base_url}/v1/models"

    headers = {}
    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'

    # 创建 SSL connector
    # verify_ssl 为 False 时禁用 SSL 验证
    ssl = False if not config['verify_ssl'] else None

    try:
        async with aiohttp.ClientSession(
            trust_env=True,
            connector=aiohttp.TCPConnector(ssl=ssl)
        ) as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return json_func(await response.json())
                else:
                    # 如果 API 调用失败，返回默认模型列表
                    return json_func({
                        'object': 'list',
                        'data': [
                            {'id': config['model'], 'object': 'model', 'created': 1234567890, 'owned_by': 'default'}
                        ]
                    })
    except Exception as e:
        logger.error(f"Failed to fetch models: {e}")
        return json_func({
            'object': 'list',
            'data': [
                {'id': config['model'], 'object': 'model', 'created': 1234567890, 'owned_by': 'default'}
            ]
        })


@expose('/api/config', methods=['GET', 'POST'])
async def api_config():
    """获取或设置配置"""
    config = get_api_config()

    if request.method == 'POST':
        try:
            json_data = await request.get_json()
        except Exception:
            json_data = {}

        # 更新配置（仅内存中）
        if json_data.get('base_url'):
            config['base_url'] = json_data['base_url']
        if json_data.get('api_key'):
            config['api_key'] = json_data['api_key']
        if json_data.get('model'):
            config['model'] = json_data['model']

        return json_func({
            'success': True,
            'config': config
        })
    else:
        # 返回配置（隐藏 api_key）
        return json_func({
            'base_url': config['base_url'],
            'model': config['model'],
            'has_api_key': bool(config['api_key']),
        })


# ============= 流式响应辅助类 =============

class StreamingResponse:
    """流式响应包装器"""

    def __init__(self, generator):
        self.generator = generator

    async def __call__(self, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'Content-Type', b'text/event-stream'],
                [b'Cache-Control', b'no-cache'],
                [b'Connection', b'keep-alive'],
            ],
        })

        async for chunk in self.generator:
            data = json.dumps(chunk, ensure_ascii=False)
            await send({
                'type': 'http.response.body',
                'body': f"data: {data}\n\n".encode('utf-8'),
            })

        await send({
            'type': 'http.response.body',
            'body': b"data: [DONE]\n\n",
        })


class SSEStreamResponse(StarletteResponse):
    """ASGI 风格的 SSE 流式响应，继承自 Starlette Response"""

    def __init__(self, message: str):
        self._message = message
        super().__init__(
            content=b'',
            media_type='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no',
            }
        )

    async def stream_response(self, send):
        """流式发送响应内容"""
        logger.info("=== SSEStreamResponse.stream_response START ===")

        try:
            # 遍历生成器并发送每个数据块
            async for data in sse_stream_generator(self._message):
                # logger.info(f"SSEStreamResponse: sending data, length={len(data)}")
                await send({
                    'type': 'http.response.body',
                    'body': data.encode('utf-8'),
                    'more_body': True,
                })

            # 发送 [DONE] 信号（标准 SSE 结束信号）
            logger.info("SSEStreamResponse: sending [DONE]")
            await send({
                'type': 'http.response.body',
                'body': b'data: [DONE]\n\n',
                'more_body': True,
            })

            # 发送空 body 表示响应结束
            logger.info("SSEStreamResponse: sending final empty body")
            await send({
                'type': 'http.response.body',
                'body': b'',
            })
        except Exception as e:
            logger.exception(f"SSEStreamResponse error: {e}")
            # 发送错误消息
            try:
                import json as json_module
                error_data = f"data: {json_module.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
                await send({
                    'type': 'http.response.body',
                    'body': error_data.encode('utf-8'),
                    'more_body': True,
                })
            except Exception:
                pass
            # 发送空 body 表示响应结束
            await send({
                'type': 'http.response.body',
                'body': b'',
            })

        logger.info("=== SSEStreamResponse.stream_response END ===")

    async def __call__(self, scope, receive, send):
        """ASGI 接口 - Starlette Response 会调用此方法"""
        logger.info("=== SSEStreamResponse.__call__ START ===")

        # 发送响应头（使用 Starlette Response 的 render 方法）
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'Content-Type', self.media_type.encode('utf-8')],
                [b'Cache-Control', b'no-cache'],
                [b'Connection', b'keep-alive'],
                [b'X-Accel-Buffering', b'no'],
            ],
        })

        # 流式发送内容
        await self.stream_response(send)

        logger.info("=== SSEStreamResponse.__call__ END ===")
