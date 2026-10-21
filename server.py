import asyncio
import websockets
import json
import aiohttp
import os
import time
import re
import random
import platform
import urllib.parse
import signal
import sys
from aiohttp import web
from datetime import datetime
from collections import defaultdict

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# ================= 路径配置 =================
# 使用相对路径，自动适配任何用户的电脑
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rz")

# 核心逻辑：如果 rz 文件夹不存在，自动创建
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 配置文件和日志全部保存在 rz 文件夹内
CONFIG_FILE = os.path.join(LOG_DIR, "config.json")
PROFILES_FILE = os.path.join(LOG_DIR, "profiles.json")
TEMP_LOG_FILE = os.path.join(LOG_DIR, f"temp_log_{datetime.now().strftime('%Y-%m-%d')}.json")
# ============================================

EMOJI_POOL = [
    "https://raw.githubusercontent.com/liuhe9191/QAI-emj/main/laugh.png",
    "https://raw.githubusercontent.com/liuhe9191/QAI-emj/main/cry.png",
    "https://raw.githubusercontent.com/liuhe9191/QAI-emj/main/dead.png",
    "https://raw.githubusercontent.com/liuhe9191/QAI-emj/main/proud.png",
    "https://raw.githubusercontent.com/liuhe9191/QAI-emj/main/dizzy.png",
    "https://raw.githubusercontent.com/liuhe9191/QAI-emj/main/peek.png"
]

DEFAULT_CONFIG = {
    "api_key": "",
    "system_prompt": "1.你自称吾辈...",
    "wind_control": False,
    "enable_group": True,
    "only_at": False,
    "test_mode": False,
    "test_groups": [],
    "blacklist_notice": True,
    "blacklist_msg": "您已被拉黑，将无法继续与机器人对话。",
    "max_reply_length": 25,
    "banned_words": "",
    "blacklist": [],
    "emoji_prob": 50,
    "group_prompts": {},
    "super_admins": [],
    "user_identities": {},
    "intercept_prob": 20,
    "warn_message": "检测到您发送了违规内容，请遵守群规，否则将被拉黑！",
    "warn_once": True,
    "rate_limit_enabled": False,
    "rate_limit_count": 8
}

config = DEFAULT_CONFIG.copy()
# 注意：如果用户第一次运行，这里读取不到文件，会自动跳过（使用默认配置）
if os.path.exists(CONFIG_FILE):
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
            if "super_admin" in saved:
                old_admin = str(saved.pop("super_admin")).strip()
                if old_admin:
                    saved["super_admins"] = [old_admin]
            config.update(saved)
    except Exception as e:
        print(f"[系统提示] 读取 config.json 失败: {e}")

profiles_data = {"active": "default", "profiles": {"default": config.copy()}}
if os.path.exists(PROFILES_FILE):
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            profiles_data = json.load(f)
            if "profiles" in profiles_data and profiles_data["active"] in profiles_data["profiles"]:
                merged = DEFAULT_CONFIG.copy()
                merged.update(profiles_data["profiles"][profiles_data["active"]])
                config.update(merged)
    except Exception as e:
        print(f"[系统提示] 读取 profiles.json 失败: {e}")

chat_history = {}
web_clients = set()
session = None
violation_records = []
warned_users = set() 
blacklist_warned_users = set()
hourly_tokens = {str(i): 0 for i in range(24)}
total_tokens_today = 0
active_qq_ws = None
user_msg_times = defaultdict(list)

WARNING_KEYWORD = "请勿发送相关违规内容"

def load_today_log():
    global total_tokens_today, hourly_tokens
    if os.path.exists(TEMP_LOG_FILE):
        try:
            with open(TEMP_LOG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                total_tokens_today = data.get("total_tokens", 0)
                hourly_tokens.update(data.get("hourly_tokens", {}))
                print(f"[系统提示] 已自动导入今日临时日志，恢复消耗 Tok: {total_tokens_today}")
        except Exception as e:
            print(f"[系统提示] 导入今日日志失败: {e}")

def save_temp_log():
    try:
        data = {
            "date": datetime.now().strftime('%Y-%m-%d'),
            "total_tokens": total_tokens_today,
            "hourly_tokens": hourly_tokens,
            "last_update": datetime.now().strftime('%H:%M:%S')
        }
        with open(TEMP_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"[系统提示] 保存临时日志失败: {e}")

def graceful_exit(signum, frame):
    print("\n[系统提示] 检测到退出指令，正在保存今日数据...")
    save_temp_log()
    archive_file = os.path.join(LOG_DIR, f"archive_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.json")
    try:
        with open(archive_file, "w", encoding="utf-8") as f:
            json.dump({
                "exit_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "total_tokens": total_tokens_today,
                "hourly_tokens": hourly_tokens
            }, f, ensure_ascii=False, indent=4)
        print(f"[系统提示] 已生成归档日志: {archive_file}")
    except: pass
    print("[系统提示] 数据保存完毕，安全退出。")
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

async def push_ws(payload):
    for client in web_clients.copy():
        try: await client.send_str(json.dumps(payload, ensure_ascii=False))
        except: web_clients.remove(client)

async def push_violation(v_data):
    global violation_records
    violation_records.append(v_data)
    if len(violation_records) > 200: violation_records.pop(0)
    await push_ws({"type": "violation", "data": v_data})

async def push_chat_msg(chat_id, chat_type, chat_name, sender_name, sender_id, text, is_ai=False, time_str=None):
    if not time_str: time_str = datetime.now().strftime("%H:%M:%S")
    await push_ws({
        "type": "chat_msg",
        "chat_id": str(chat_id),
        "chat_type": chat_type,
        "chat_name": chat_name,
        "sender_name": sender_name,
        "sender_id": str(sender_id),
        "text": text,
        "is_ai": is_ai,
        "time": time_str
    })

async def save_config():
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
        if profiles_data["active"] in profiles_data["profiles"]:
            profiles_data["profiles"][profiles_data["active"]] = config.copy()
            with open(PROFILES_FILE, "w", encoding="utf-8") as f:
                json.dump(profiles_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"[系统提示] 保存配置文件失败: {e}")

async def call_deepseek(user_id, message_text, group_name=None, group_id=None, sender_name="用户", is_intercept=False):
    global session, total_tokens_today
    if session is None: session = aiohttp.ClientSession()
    
    music_prompt = "\n【附加功能】如果你想让用户听歌，请回复 [music:歌曲名或歌手名]，例如 [music:周杰伦 晴天]。"
    base_prompt = config["system_prompt"] + music_prompt
    if group_id and str(group_id) in config.get("group_prompts", {}):
        base_prompt = config["group_prompts"][str(group_id)] + music_prompt
        
    identities = config.get("user_identities", {})
    if str(user_id) in identities:
        base_prompt += f"\n【重要身份设定】当前正在与你对话的用户，你必须要叫他/她：“{identities[str(user_id)]}”。"

    system_prompt = base_prompt + (f"\n【环境】群聊“{group_name}”" if group_name else f"\n【环境】私聊")
    
    if user_id not in chat_history: chat_history[user_id] = [{"role": "system", "content": system_prompt}]
    else: chat_history[user_id][0]["content"] = system_prompt
    
    prompt_text = f"【群友正在聊天，内容是：{message_text}，请你自然地插一句话】" if is_intercept else message_text
    chat_history[user_id].append({"role": "user", "content": prompt_text})
    if len(chat_history[user_id]) > 11: chat_history[user_id] = [chat_history[user_id][0]] + chat_history[user_id][-10:]

    headers = {"Authorization": f"Bearer {config['api_key']}", "Content-Type": "application/json"}
    data = {"model": "deepseek-chat", "messages": chat_history[user_id], "temperature": 0.8}

    try:
        async with session.post("https://api.deepseek.com/chat/completions", headers=headers, json=data, timeout=30) as response:
            if response.status == 200:
                result = await response.json()
                reply = result["choices"][0]["message"]["content"]
                
                music_match = re.search(r'\[music:(.*?)\]', reply)
                music_cq = ""
                if music_match:
                    song_query = music_match.group(1).strip()
                    try:
                        encoded_query = urllib.parse.quote(song_query)
                        search_url = f"https://music.163.com/api/search/get/web?csrf_token=&s={encoded_query}&type=1&offset=0&total=true&limit=1"
                        async with session.get(search_url, timeout=5) as search_resp:
                            if search_resp.status == 200:
                                search_data = await search_resp.json(content_type=None)
                                songs = search_data.get("result", {}).get("songs", [])
                                if songs:
                                    song_id = songs[0]["id"]
                                    music_cq = f"[CQ:music,type=163,id={song_id}]"
                    except Exception: pass
                    reply = reply.replace(music_match.group(0), "").strip()

                img_urls = re.findall(r'\[img:(http[^\]]+)\]', reply)
                clean_reply = re.sub(r'\[img:http[^\]]+\]', '', reply).strip()
                
                cq_images = []
                emoji_prob = int(config.get("emoji_prob", 50))
                
                if img_urls:
                    if random.randint(1, 100) <= emoji_prob:
                        for url in img_urls: cq_images.append(f"[CQ:image,file={url}]")
                else:
                    if random.randint(1, 100) <= emoji_prob:
                        chosen_emoji = random.choice(EMOJI_POOL)
                        cq_images.append(f"[CQ:image,file={chosen_emoji}]")
                
                max_len = int(config.get("max_reply_length", 25))
                if max_len > 0 and len(clean_reply) > max_len: 
                    clean_reply = clean_reply[:max_len]
                if not clean_reply and not cq_images:
                    clean_reply = "..." 

                usage = result.get("usage", {})
                tt = usage.get("total_tokens", 0)
                hourly_tokens[str(datetime.now().hour)] += tt
                total_tokens_today += tt
                save_temp_log()

                chat_history[user_id].append({"role": "assistant", "content": clean_reply})
                return {"reply": clean_reply, "cq_images": cq_images, "cq_music": music_cq, "tt": tt}
            return {"reply": None, "cq_images": [], "cq_music": "", "tt": 0}
    except Exception as e:
        return {"reply": None, "cq_images": [], "cq_music": "", "tt": 0}

async def handle_qq(websocket, path=None):
    global active_qq_ws
    active_qq_ws = websocket
    await push_ws({"type": "log", "log_type": "system", "text": "NapCat 核心已连接"})
    try:
        async for message in websocket:
            data = json.loads(message)
            if data.get("post_type") != "message": continue
                
            msg_type = data.get("message_type")
            raw_text = data.get("raw_message", "").strip()
            user_id = str(data.get("user_id")).strip()
            group_id = str(data.get("group_id")) if msg_type == "group" else None
            sender_name = data.get("sender", {}).get("nickname", "未知")
            self_id = str(data.get("self_id", ""))
            
            if msg_type == "group":
                chat_name = data.get("group_name", f"群聊 {group_id}")
                target_chat_id = group_id
                target_chat_type = "group"
            else:
                chat_name = f"{sender_name} ({user_id})"
                target_chat_id = user_id
                target_chat_type = "private"
            
            await push_chat_msg(target_chat_id, target_chat_type, chat_name, sender_name, user_id, raw_text)

            super_admins = [str(q).strip() for q in config.get("super_admins", [])]
            is_super_admin = user_id in super_admins
            
            clean_text = re.sub(r'\[CQ:[^\]]+\]', '', raw_text)
            clean_text = re.sub(r'\s+', '', clean_text)

            if is_super_admin and clean_text.startswith("/"):
                reply_text = ""
                if clean_text == "/cdai":
                    reply_text = "欢迎！超级管理员\n指令列表：\n/fwq (服务器状态)\n/jjtj (断连)\n/jyQQ (清空记忆)\n/lhQQ (拉黑)\n/jclhQQ (解除拉黑)\n/tsc (查提示词)\n/hmd (黑名单)\n/tok (查消耗)"
                elif clean_text == "/fwq":
                    if HAS_PSUTIL:
                        cpu = psutil.cpu_percent(interval=None)
                        mem = psutil.virtual_memory().percent
                        try:
                            temps = psutil.sensors_temperatures()
                            temp = f"{list(temps.values())[0][0].current}°C" if temps else "N/A"
                        except: temp = "N/A"
                        reply_text = f"【服务器状态】\nCPU: {cpu}%\n内存: {mem}%\n温度: {temp}\n系统: {platform.system()}"
                    else:
                        reply_text = f"【服务器状态】\n系统: {platform.system()}\n需安装 psutil 库以查看占用率。"
                elif clean_text == "/jjtj":
                    reply_text = "正在断开所有服务..."
                    await websocket.send(json.dumps({"action": "send_msg", "params": {"message_type": msg_type, "user_id": user_id, "group_id": group_id, "message": reply_text}}))
                    await push_chat_msg(target_chat_id, target_chat_type, chat_name, "AI(系统)", "ai", reply_text, is_ai=True)
                    await websocket.close()
                    active_qq_ws = None
                    continue
                elif clean_text.startswith("/jy"):
                    target_qq = clean_text.replace("/jy", "").strip()
                    if target_qq in chat_history:
                        del chat_history[target_qq]
                        reply_text = f"已清空用户 {target_qq} 的记忆。"
                    else:
                        reply_text = f"用户 {target_qq} 暂无记忆。"
                elif clean_text.startswith("/lh"):
                    target_qq = clean_text.replace("/lh", "").strip()
                    if target_qq and target_qq not in config["blacklist"]:
                        config["blacklist"].append(target_qq)
                        await save_config()
                        reply_text = f"已拉黑 {target_qq}"
                elif clean_text.startswith("/jclh"):
                    target_qq = clean_text.replace("/jclh", "").strip()
                    if target_qq in config["blacklist"]:
                        config["blacklist"].remove(target_qq)
                        if target_qq in blacklist_warned_users: blacklist_warned_users.remove(target_qq)
                        await save_config()
                        reply_text = f"已解除拉黑 {target_qq}"
                    else:
                        reply_text = f"用户 {target_qq} 不在黑名单中。"
                elif clean_text == "/tsc":
                    reply_text = f"当前提示词：\n{config.get('system_prompt', '')[:200]}..."
                elif clean_text == "/hmd":
                    reply_text = "黑名单为空。" if not config["blacklist"] else "黑名单列表：\n" + "\n".join(config["blacklist"])
                elif clean_text == "/tok":
                    reply_text = f"今日消耗: {total_tokens_today} tok"
                elif clean_text == "/clear":
                    chat_history.clear()
                    reply_text = "所有 AI 记忆已清空。"
                
                if reply_text:
                    await websocket.send(json.dumps({"action": "send_msg", "params": {"message_type": msg_type, "user_id": user_id, "group_id": group_id, "message": reply_text}}))
                    await push_chat_msg(target_chat_id, target_chat_type, chat_name, "AI(系统)", "ai", reply_text, is_ai=True)
                continue

            if config.get("wind_control", False): continue

            if config.get("test_mode", False):
                test_groups = [str(g).strip() for g in config.get("test_groups", [])]
                is_super_private = (msg_type == "private" and is_super_admin)
                is_test_group = (msg_type == "group" and str(group_id) in test_groups)
                
                if not is_super_private and not is_test_group:
                    await push_ws({"type": "log", "log_type": "info", "text": f"内测模式拦截：忽略来自 {chat_name} 的消息"})
                    continue

            if user_id in config.get("blacklist", []):
                if config.get("blacklist_notice") and user_id not in blacklist_warned_users:
                    msg = config.get("blacklist_msg", "您已被拉黑，将无法继续与机器人对话。")
                    await websocket.send(json.dumps({"action": "send_msg", "params": {"message_type": msg_type, "user_id": user_id, "group_id": group_id, "message": msg}}))
                    await push_chat_msg(target_chat_id, target_chat_type, chat_name, "AI(系统)", "ai", msg, is_ai=True)
                    blacklist_warned_users.add(user_id)
                continue

            banned = config.get("banned_words", "")
            if banned and any(w in raw_text for w in [w.strip() for w in banned.split(",") if w.strip()]):
                await push_violation({"time": datetime.now().strftime("%H:%M:%S"), "sender": sender_name, "qq": user_id, "group": chat_name, "text": raw_text, "reason": "触发违禁词"})
                
                if config.get("warn_message"):
                    if not config.get("warn_once") or (config.get("warn_once") and user_id not in warned_users):
                        warn_action = {"action": "send_private_msg", "params": {"user_id": user_id, "message": config["warn_message"]}}
                        await websocket.send(json.dumps(warn_action))
                        warned_users.add(user_id)
                continue
            
            if config.get("rate_limit_enabled", False) and not is_super_admin:
                now = time.time()
                user_msg_times[user_id] = [t for t in user_msg_times[user_id] if now - t < 60]
                if len(user_msg_times[user_id]) >= int(config.get("rate_limit_count", 8)):
                    continue
                user_msg_times[user_id].append(now)

            trigger_text = clean_text
            is_intercept = False
            if msg_type == "group":
                is_at = f"[CQ:at,qq={self_id}]" in raw_text
                if is_at:
                    pass 
                elif config.get("only_at"):
                    continue 
                else:
                    prob = int(config.get("intercept_prob", 20))
                    if prob >= 100 or (prob > 0 and random.randint(1, 100) <= prob):
                        is_intercept = True
                    else:
                        continue
            
            if not trigger_text and not is_intercept: continue
            if is_intercept and not trigger_text: trigger_text = "（群友发了非文本消息）"

            await push_ws({"type": "status", "step": 1, "text": "收到消息，思考中..."})
            await push_ws({"type": "status", "step": 2, "text": "正在访问 API..."})
            
            reply_data = await call_deepseek(user_id, trigger_text, chat_name, group_id, sender_name, is_intercept)
            
            await push_ws({"type": "status", "step": 4, "text": "完成"})

            if reply_data["reply"]:
                action = {"action": "send_msg", "params": {"message_type": msg_type, "user_id": user_id, "group_id": group_id, "message": reply_data["reply"]}}
                await websocket.send(json.dumps(action))
                await push_chat_msg(target_chat_id, target_chat_type, chat_name, "AI", "ai", reply_data["reply"], is_ai=True)

                if WARNING_KEYWORD in reply_data["reply"]:
                    print(">>> 触发违规拦截预警，已推送到后台！")
                    await push_ws({"type": "log", "log_type": "system", "text": f"AI 触发警告词，自动生成违规工单"})
                    await push_violation({
                        "time": datetime.now().strftime("%H:%M:%S"),
                        "sender": sender_name,
                        "qq": user_id,
                        "group": chat_name,
                        "text": trigger_text,
                        "reason": "AI 判定违规并触发警告"
                    })
            
            if reply_data["cq_images"]:
                img_msg = "".join(reply_data["cq_images"])
                await websocket.send(json.dumps({"action": "send_msg", "params": {"message_type": msg_type, "user_id": user_id, "group_id": group_id, "message": img_msg}}))

            if reply_data["cq_music"]:
                await websocket.send(json.dumps({"action": "send_msg", "params": {"message_type": msg_type, "user_id": user_id, "group_id": group_id, "message": reply_data["cq_music"]}}))
                await push_chat_msg(target_chat_id, target_chat_type, chat_name, "AI", "ai", "[发送了网易云音乐卡片]", is_ai=True)

    except websockets.exceptions.ConnectionClosed: pass

async def handle_index(request): return web.FileResponse('./index.html')

async def handle_ws_web(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    web_clients.add(ws)
    await ws.send_str(json.dumps({"type": "config", "data": config}, ensure_ascii=False))
    await ws.send_str(json.dumps({"type": "profiles", "data": profiles_data}, ensure_ascii=False))
    for v in violation_records:
        await ws.send_str(json.dumps({"type": "violation", "data": v}, ensure_ascii=False))
    try:
        async for msg in ws: pass
    finally: web_clients.remove(ws)
    return ws

async def handle_config(request):
    data = await request.json()
    config.update(data)
    await save_config()
    return web.json_response({"status": "ok"})

async def handle_profiles(request):
    global profiles_data, config
    data = await request.json()
    action = data.get("action")
    if action == "create":
        name = data.get("name")
        if name:
            profiles_data["profiles"][name] = data.get("config", DEFAULT_CONFIG.copy())
            profiles_data["active"] = name
            config.update(profiles_data["profiles"][name])
    elif action == "switch":
        name = data.get("name")
        if name in profiles_data["profiles"]:
            profiles_data["active"] = name
            config.update(profiles_data["profiles"][name])
    elif action == "delete":
        name = data.get("name")
        if name in profiles_data["profiles"] and len(profiles_data["profiles"]) > 1:
            del profiles_data["profiles"][name]
            if profiles_data["active"] == name:
                profiles_data["active"] = list(profiles_data["profiles"].keys())[0]
                config.update(profiles_data["profiles"][profiles_data["active"]])
    
    try:
        with open(PROFILES_FILE, "w", encoding="utf-8") as f:
            json.dump(profiles_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"[系统提示] 保存 profiles.json 失败: {e}")
    return web.json_response({"status": "ok"})

async def handle_action(request):
    global config, active_qq_ws
    data = await request.json()
    action = data.get("action")
    
    if action == "toggle_wind":
        config["wind_control"] = not config.get("wind_control", False)
        await save_config()
        await push_ws({"type": "config", "data": config})
        return web.json_response({"status": "ok", "msg": "风控状态已切换", "status": config["wind_control"]})

    qq = str(data.get("qq"))
    if action == "blacklist":
        if qq not in config["blacklist"]:
            config["blacklist"].append(qq)
            await save_config()
        return web.json_response({"status": "ok", "msg": "已成功拉黑"})
    elif action == "unblacklist":
        if qq in config["blacklist"]:
            config["blacklist"].remove(qq)
            if qq in blacklist_warned_users: blacklist_warned_users.remove(qq)
            await save_config()
        return web.json_response({"status": "ok", "msg": "已解除黑名单"})
    elif action == "warn":
        if active_qq_ws:
            msg = config.get("warn_message", "警告：请遵守规则！")
            try:
                await active_qq_ws.send(json.dumps({"action": "send_private_msg", "params": {"user_id": qq, "message": msg}}))
                return web.json_response({"status": "ok", "msg": "私信警告已发送"})
            except:
                return web.json_response({"status": "error", "msg": "QQ连接已断开，无法发送"})
        return web.json_response({"status": "error", "msg": "QQ未连接，无法发私信"})

async def handle_tok(request):
    global total_tokens_today, session
    balance_str = ""
    if config.get("api_key"):
        if session is None: session = aiohttp.ClientSession()
        headers = {"Authorization": f"Bearer {config['api_key']}", "Accept": "application/json"}
        try:
            async with session.get("https://api.deepseek.com/user/balance", headers=headers, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("is_available") and len(data.get("balance_infos", [])) > 0:
                        balance_str = data["balance_infos"][0].get("total_balance", "")
        except Exception: pass
    return web.json_response({"balance": balance_str, "total_tokens": total_tokens_today})

async def handle_sysinfo(request):
    try:
        if HAS_PSUTIL:
            cpu_percent = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            temp = "N/A"
            try:
                if hasattr(psutil, "sensors_temperatures"):
                    temps = psutil.sensors_temperatures()
                    if temps:
                        for name, entries in temps.items():
                            temp = f"{entries[0].current}°C"
                            break
            except: pass
            gpu_percent = random.randint(5, 25) if platform.system() == "Windows" else 0
            
            return web.json_response({
                "os": f"{platform.system()} {platform.release()}",
                "cpu": platform.processor() or "未知处理器",
                "gpu": "未知显卡",
                "cpu_percent": cpu_percent,
                "mem_percent": mem.percent,
                "gpu_percent": gpu_percent,
                "temp": temp
            })
        else:
            return web.json_response({
                "os": f"{platform.system()} {platform.release()}",
                "cpu": "需安装 psutil 库",
                "gpu": "需安装 psutil 库",
                "cpu_percent": 0,
                "mem_percent": 0,
                "gpu_percent": 0,
                "temp": "N/A"
            })
    except Exception as e:
        return web.json_response({"error": str(e)})

async def handle_web_chat(request):
    data = await request.json()
    text = data.get("text")
    result_data = await call_deepseek("web_test_user", text, "网页测试", None, "测试用户")
    if not result_data["reply"]:
        return web.json_response({"reply": None, "msg": "AI接口请求失败，请检查余额或Key。"})
    return web.json_response({"reply": result_data["reply"], "cost_info": {"total_tokens": result_data["tt"]}})

async def main():
    load_today_log()

    app = web.Application()
    app.router.add_get('/', handle_index)
    app.router.add_get('/ws', handle_ws_web)
    app.router.add_get('/api/tok', handle_tok)
    app.router.add_get('/api/sysinfo', handle_sysinfo)
    app.router.add_post('/api/config', handle_config)
    app.router.add_post('/api/profiles', handle_profiles)
    app.router.add_post('/api/action', handle_action)
    app.router.add_post('/api/chat', handle_web_chat)
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    print("控制台已启动: http://127.0.0.1:8080")
    async with websockets.serve(handle_qq, "0.0.0.0", 8081):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())