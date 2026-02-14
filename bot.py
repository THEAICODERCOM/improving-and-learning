import discord
from discord.ext import commands, tasks
from discord import app_commands
import os
import random
import time
import aiosqlite
import json
import ssl
import aiohttp
import asyncio
import datetime
from dotenv import load_dotenv
import uuid

# Load environment variables
load_dotenv(override=True)
TOKEN = os.getenv('DISCORD_TOKEN')

# FIX: macOS SSL Certificate verification bug
# This is the most aggressive way to bypass the macOS certificate issue
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# Monkeypatch aiohttp to completely disable SSL verification for Discord
orig_request = aiohttp.ClientSession._request
async def new_request(self, method, url, *args, **kwargs):
    kwargs['ssl'] = False
    return await orig_request(self, method, url, *args, **kwargs)
aiohttp.ClientSession._request = new_request

orig_ws_connect = aiohttp.ClientSession.ws_connect
async def new_ws_connect(self, url, *args, **kwargs):
    kwargs['ssl'] = False
    return await orig_ws_connect(self, url, *args, **kwargs)
aiohttp.ClientSession.ws_connect = new_ws_connect

# Database Setup
DB_FILE = os.getenv('DB_FILE', 'empire_v2.db')
DAILY_QUESTS = [
    {"id": "daily_cmd_25", "description": "Use 25 commands today", "target": 25, "reward": 10000},
    {"id": "daily_cmd_50", "description": "Use 50 commands today", "target": 50, "reward": 20000},
    {"id": "daily_cmd_75", "description": "Use 75 commands today", "target": 75, "reward": 35000},
    {"id": "daily_cmd_100", "description": "Use 100 commands today", "target": 100, "reward": 50000},
    {"id": "daily_cmd_150", "description": "Use 150 commands today", "target": 150, "reward": 80000},
    {"id": "daily_cmd_10", "description": "Use 10 commands today", "target": 10, "reward": 5000},
    {"id": "daily_cmd_5", "description": "Use 5 commands today", "target": 5, "reward": 2000},
    {"id": "daily_cmd_200", "description": "Use 200 commands today", "target": 200, "reward": 120000},
    {"id": "daily_work_10", "description": "Work 10 times", "target": 10, "reward": 25000, "kind": "work"},
    {"id": "daily_crime_3", "description": "Succeed 3 crimes", "target": 3, "reward": 30000, "kind": "crime_success"},
    {"id": "daily_blackjack_3", "description": "Win 3 blackjack games", "target": 3, "reward": 35000, "kind": "blackjack_wins"},
    {"id": "daily_rob_2", "description": "Successfully rob 2 users", "target": 2, "reward": 40000, "kind": "rob_success"},
]
WEEKLY_QUESTS = [
    {"id": "weekly_cmd_100", "description": "Use 100 commands this week", "target": 100, "reward": 40000},
    {"id": "weekly_cmd_200", "description": "Use 200 commands this week", "target": 200, "reward": 90000},
    {"id": "weekly_cmd_300", "description": "Use 300 commands this week", "target": 300, "reward": 140000},
    {"id": "weekly_cmd_400", "description": "Use 400 commands this week", "target": 400, "reward": 190000},
    {"id": "weekly_cmd_500", "description": "Use 500 commands this week", "target": 500, "reward": 250000},
    {"id": "weekly_cmd_750", "description": "Use 750 commands this week", "target": 750, "reward": 375000},
    {"id": "weekly_cmd_50", "description": "Use 50 commands this week", "target": 50, "reward": 25000},
    {"id": "weekly_cmd_1000", "description": "Use 1000 commands this week", "target": 1000, "reward": 500000},
    {"id": "weekly_work_50", "description": "Work 50 times", "target": 50, "reward": 150000, "kind": "work"},
    {"id": "weekly_crime_15", "description": "Succeed 15 crimes", "target": 15, "reward": 200000, "kind": "crime_success"},
    {"id": "weekly_blackjack_20", "description": "Win 20 blackjack games", "target": 20, "reward": 220000, "kind": "blackjack_wins"},
    {"id": "weekly_rob_10", "description": "Successfully rob 10 users", "target": 10, "reward": 250000, "kind": "rob_success"},
]
TEST_GUILD_ID = 1465437620245889237
SUPPORT_SERVER_INVITE = "BkCxVgJa"
SUPPORT_GUILD_ID = None
BOT_OWNERS = [1324354578338025533]
INSTANCE_ID = None
LOCK_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_LOCK_PATH = os.path.join(LOCK_DIR, ".empire_instance.lock")

DEFAULT_BANK_PLANS = {
    "standard": {
        "name": "Standard Vault",
        "min": 0.01,
        "max": 0.02,
        "price": 0,
        "min_level": 0
    },
    "premium": {
        "name": "Premium Vault",
        "min": 0.02,
        "max": 0.03,
        "price": 25000,
        "min_level": 10
    },
    "royal": {
        "name": "Royal Vault",
        "min": 0.03,
        "max": 0.05,
        "price": 100000,
        "min_level": 25
    }
}
DEFAULT_ASSETS = {
    "lemonade_stand": {"name": "Lemonade Stand", "price": 500, "income": 5},
    "gaming_pc": {"name": "Gaming PC", "price": 2500, "income": 30},
    "coffee_shop": {"name": "Coffee Shop", "price": 10000, "income": 150},
}

# Boss System: Loot items and boss definitions
LOOT_ITEMS = {
    "healing_potion": {"name": "Healing Potion", "rarity": "common", "value": 1000, "effect": "heal_10"},
    "minor_damage_booster": {"name": "Minor Damage Booster", "rarity": "common", "value": 2500, "effect": "boost_5"},
    "silver_coin": {"name": "Silver Coin", "rarity": "uncommon", "value": 50000, "effect": "wallet_add"},
    "gold_coin": {"name": "Gold Coin", "rarity": "uncommon", "value": 150000, "effect": "wallet_add"},
    "rare_gem": {"name": "Rare Gem", "rarity": "rare", "value": 500000, "effect": "sellable"},
    "mana_elixir": {"name": "Mana Elixir", "rarity": "rare", "value": 500000, "effect": "boost_20"},
    "epic_sword": {"name": "Epic Sword", "rarity": "epic", "value": 1000000, "effect": "boost_50"},
    "legendary_amulet": {"name": "Legendary Amulet", "rarity": "legendary", "value": 2500000, "effect": "auto_attack_5hp"},
    "shield_of_fortitude": {"name": "Shield of Fortitude", "rarity": "rare", "value": 750000, "effect": "reduce_incoming"},
    "boss_trophy": {"name": "Boss Trophy", "rarity": "epic", "value": 1500000, "effect": "cosmetic"},
    "lucky_charm": {"name": "Lucky Charm", "rarity": "uncommon", "value": 100000, "effect": "luck_10"},
    "crystal_of_rage": {"name": "Crystal of Rage", "rarity": "epic", "value": 1000000, "effect": "double_next"},
    "mystic_scroll": {"name": "Mystic Scroll", "rarity": "rare", "value": 500000, "effect": "reveal_hp"},
    "treasure_chest": {"name": "Treasure Chest", "rarity": "legendary", "value": 3000000, "effect": "random_loot"},
    "coin_multiplier": {"name": "Coin Multiplier", "rarity": "epic", "value": 1500000, "effect": "double_rewards"}
}

BOSSES = [
    {"name": "Robo-King", "difficulty": "Medium", "hp": 5_000_000, "features": ["reflect_5"], "loot": ["healing_potion","silver_coin","rare_gem"]},
    {"name": "Shadow Serpent", "difficulty": "Hard", "hp": 10_000_000, "features": ["reduce_10"], "loot": ["mana_elixir","rare_gem","epic_sword"]},
    {"name": "Flame Golem", "difficulty": "Medium", "hp": 5_000_000, "features": ["fire_aura_10"], "loot": ["silver_coin","rare_gem","lucky_charm"]},
    {"name": "Frost Titan", "difficulty": "Expert", "hp": 25_000_000, "features": ["freeze_5s"], "loot": ["epic_sword","legendary_amulet"]},
    {"name": "Dark Phantom", "difficulty": "Hard", "hp": 10_000_000, "features": ["evade_10"], "loot": ["mana_elixir","lucky_charm","epic_sword"]},
    {"name": "Golden Dragon", "difficulty": "Expert", "hp": 25_000_000, "features": ["double_coin_drops"], "loot": ["legendary_amulet","treasure_chest"]},
    {"name": "Vicious Wolf", "difficulty": "Easy", "hp": 1_000_000, "features": ["fast_regen"], "loot": ["healing_potion","silver_coin"]},
    {"name": "Cursed Knight", "difficulty": "Medium", "hp": 5_000_000, "features": ["curse_wallet_5"], "loot": ["rare_gem","crystal_of_rage"]},
    {"name": "Thunder Beast", "difficulty": "Hard", "hp": 10_000_000, "features": ["strike_back_5"], "loot": ["epic_sword","lucky_charm"]},
    {"name": "Toxic Slime", "difficulty": "Easy", "hp": 1_000_000, "features": ["poison"], "loot": ["healing_potion","silver_coin"]},
    {"name": "Phantom Mage", "difficulty": "Medium", "hp": 5_000_000, "features": ["shield_5"], "loot": ["mana_elixir","mystic_scroll"]},
    {"name": "Colossal Ogre", "difficulty": "Medium", "hp": 5_000_000, "features": ["high_def_90"], "loot": ["rare_gem","epic_sword"]},
    {"name": "Shadow Hydra", "difficulty": "Expert", "hp": 25_000_000, "features": ["split_heads"], "loot": ["legendary_amulet","treasure_chest"]},
    {"name": "Iron Golem", "difficulty": "Hard", "hp": 10_000_000, "features": ["reduce_20"], "loot": ["crystal_of_rage","epic_sword"]},
    {"name": "Arcane Elemental", "difficulty": "Medium", "hp": 5_000_000, "features": ["random_boost_reduce"], "loot": ["healing_potion","silver_coin","rare_gem"]}
]
JOBS = {
    "miner": {
        "name": "Mine Overseer",
        "difficulty": "Easy",
        "min_level": 1,
        "focus": "work",
        "question": "Which command lets you supervise the mines for coins?",
        "answer": "work",
        "multiplier": 1.2
    },
    "enforcer": {
        "name": "City Enforcer",
        "difficulty": "Medium",
        "min_level": 5,
        "focus": "crime",
        "question": "Which command do you use to attempt a high-risk heist?",
        "answer": "crime",
        "multiplier": 1.3
    },
    "croupier": {
        "name": "Casino Croupier",
        "difficulty": "Hard",
        "min_level": 10,
        "focus": "blackjack",
        "question": "Which command starts a game of blackjack?",
        "answer": "blackjack",
        "multiplier": 1.4
    }
}

class EconomyService:
    async def ensure_user(self, user_id: int, guild_id: int):
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    await db.execute('INSERT OR IGNORE INTO users (user_id, guild_id) VALUES (?, ?)', (user_id, guild_id))
                    await db.commit()
                return
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                raise
    async def add_money(self, user_id: int, guild_id: int, amount: int):
        await self.ensure_user(user_id, guild_id)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    await db.execute('UPDATE users SET balance = COALESCE(balance,0) + ? WHERE user_id = ? AND guild_id = ?', (amount, user_id, guild_id))
                    await db.commit()
                return True
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False
        return False
    async def get_global_money(self, user_id: int):
        await self.ensure_user(user_id, 0)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    db.row_factory = aiosqlite.Row
                    async with db.execute('SELECT user_id, guild_id, balance, bank, bank_plan, last_work, last_crime, last_rob FROM users WHERE user_id = ? AND guild_id = 0', (user_id,)) as cursor:
                        row = await cursor.fetchone()
                        if row:
                            return row
                    async with db.execute('SELECT COALESCE(SUM(balance),0), COALESCE(SUM(bank),0) FROM users WHERE user_id = ?', (user_id,)) as cursor:
                        agg = await cursor.fetchone()
                    balance_sum = int((agg[0] or 0))
                    bank_sum = int((agg[1] or 0))
                    await db.execute('INSERT OR REPLACE INTO users (user_id, guild_id, balance, bank, bank_plan) VALUES (?, 0, ?, ?, ?)', (user_id, balance_sum, bank_sum, 'standard'))
                    await db.commit()
                async with aiosqlite.connect(DB_FILE) as db2:
                    await db2.execute('PRAGMA busy_timeout=2000')
                    db2.row_factory = aiosqlite.Row
                    async with db2.execute('SELECT user_id, guild_id, balance, bank, bank_plan, last_work, last_crime, last_rob FROM users WHERE user_id = ? AND guild_id = 0', (user_id,)) as cursor2:
                        return await cursor2.fetchone()
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                raise
    async def move_global_wallet_to_bank(self, user_id: int, amount: int):
        await self.ensure_user(user_id, 0)
        amt = int(amount)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    await db.execute('UPDATE users SET balance = MAX(0, COALESCE(balance,0) - ?), bank = COALESCE(bank,0) + ? WHERE user_id = ? AND guild_id = 0', (amt, amt, user_id))
                    await db.commit()
                return True
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False
        return False
    async def move_global_bank_to_wallet(self, user_id: int, amount: int):
        await self.ensure_user(user_id, 0)
        amt = int(amount)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    await db.execute('UPDATE users SET bank = MAX(0, COALESCE(bank,0) - ?), balance = COALESCE(balance,0) + ? WHERE user_id = ? AND guild_id = 0', (amt, amt, user_id))
                    await db.commit()
                return True
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False
        return False
    async def switch_bank_plan(self, user_id: int, plan_id: str, price: int):
        await self.ensure_user(user_id, 0)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    if price > 0:
                        await db.execute('UPDATE users SET balance = MAX(0, COALESCE(balance,0) - ?), bank_plan = ? WHERE user_id = ? AND guild_id = 0', (price, plan_id, user_id))
                    else:
                        await db.execute('UPDATE users SET bank_plan = ? WHERE user_id = ? AND guild_id = 0', (plan_id, user_id))
                    await db.commit()
                return True
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False
        return False
    async def transfer_global_balance(self, sender_id: int, receiver_id: int, amount: int):
        await self.ensure_user(sender_id, 0)
        await self.ensure_user(receiver_id, 0)
        amt = int(amount)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    await db.execute('UPDATE users SET balance = MAX(0, COALESCE(balance,0) - ?) WHERE user_id = ? AND guild_id = 0', (amt, sender_id))
                    await db.execute('UPDATE users SET balance = COALESCE(balance,0) + ? WHERE user_id = ? AND guild_id = 0', (amt, receiver_id))
                    await db.commit()
                return True
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False
        return False
    async def rob_user(self, stealer_id: int, victim_id: int):
        stealer = await self.get_global_money(stealer_id)
        victim = await self.get_global_money(victim_id)
        now = int(time.time())
        if victim['balance'] < 500:
            return False, "Target is too poor! They need at least 500 coins."
        if now - int(stealer['last_rob'] or 0) < 1800:
            return False, f"Wait {1800 - (now - int(stealer['last_rob'] or 0))}s."
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    if random.random() < 0.35:
                        stolen = random.randint(50, max(50, int(victim['balance'] * 0.25)))
                        await db.execute('UPDATE users SET balance = COALESCE(balance,0) + ?, last_rob = ?, successful_robs = COALESCE(successful_robs,0) + 1 WHERE user_id = ? AND guild_id = 0', (stolen, now, stealer_id))
                        await db.execute('UPDATE users SET balance = MAX(0, COALESCE(balance,0) - ?) WHERE user_id = ? AND guild_id = 0', (stolen, victim_id))
                        await db.commit()
                        return True, stolen
                    else:
                        fine = random.randint(300, 600)
                        await db.execute('UPDATE users SET balance = MAX(0, COALESCE(balance,0) - ?), last_rob = ? WHERE user_id = ? AND guild_id = 0', (fine, now, stealer_id))
                        await db.commit()
                        return False, fine
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False, "error"
        return False, "error"
    async def crime_action(self, user_id: int):
        data = await self.get_global_money(user_id)
        now = int(time.time())
        if now - int(data['last_crime'] or 0) < 1800:
            return False, f"🚔 Cops are searching for you! Wait {1800 - (now - int(data['last_crime'] or 0))}s."
        base = random.randint(1000, 3000) * int(data['level'] or 1)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    if random.random() < 0.30:
                        await db.execute('UPDATE users SET balance = COALESCE(balance,0) + ?, last_crime = ?, successful_crimes = COALESCE(successful_crimes,0) + 1 WHERE user_id = ? AND guild_id = 0', (base, now, user_id))
                        await db.commit()
                        return True, base
                    else:
                        loss = random.randint(500, 1000)
                        await db.execute('UPDATE users SET balance = MAX(0, COALESCE(balance,0) - ?), last_crime = ? WHERE user_id = ? AND guild_id = 0', (loss, now, user_id))
                        await db.commit()
                        return False, loss
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False, "error"
        return False, "error"
    async def add_xp_and_level(self, user_id: int, guild_id: int, amount: int):
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    await db.execute('UPDATE users SET xp = COALESCE(xp,0) + ? WHERE user_id = ? AND guild_id = ?', (amount, user_id, guild_id))
                    await db.commit()
                    async with db.execute('SELECT xp, level FROM users WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
                        row = await cursor.fetchone()
                        if row:
                            current_xp = int(row[0] or 0)
                            current_level = int(row[1] or 1)
                            next_level_xp = max(100, current_level * 100)
                            if current_xp >= next_level_xp:
                                new_level = current_level + 1
                                await db.execute('UPDATE users SET level = ?, xp = xp - ? WHERE user_id = ? AND guild_id = ?', (new_level, next_level_xp, user_id, guild_id))
                                await db.commit()
                                return True, new_level
                break
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                raise
        return False, None
    async def work_action(self, user_id: int, guild_id: int):
        data = await self.get_global_money(user_id)
        now = int(time.time())
        if now - int(data['last_work'] or 0) < 300:
            return False, f"⏳ Your workers are tired! Wait {300 - (now - int(data['last_work'] or 0))}s."
        base = random.randint(100, 300) * int(data['level'] or 1) * (int(data['prestige'] or 0) + 1)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    await db.execute('UPDATE users SET balance = COALESCE(balance,0) + ?, last_work = ? WHERE user_id = ? AND guild_id = 0', (base, now, user_id))
                    await db.commit()
                leveled_up, new_level = await self.add_xp_and_level(user_id, guild_id, 20)
                return True, base, leveled_up, new_level
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False, "error", False, None
        return False, "error", False, None
    def _pick_daily(self, guild_id: int, timestamp: int | None = None):
        ts = int(time.time()) if timestamp is None else int(timestamp)
        day = ts // 86400
        seed = f"{guild_id}-{day}-daily"
        rng = random.Random(seed)
        pool = list(DAILY_QUESTS)
        rng.shuffle(pool)
        return pool[:3]
    def _pick_weekly(self, guild_id: int, timestamp: int | None = None):
        ts = int(time.time()) if timestamp is None else int(timestamp)
        week = ts // 604800
        seed = f"{guild_id}-{week}-weekly"
        rng = random.Random(seed)
        pool = list(WEEKLY_QUESTS)
        rng.shuffle(pool)
        return pool[:3]
    async def ensure_quest_resets(self, user_id: int, guild_id: int):
        await self.ensure_user(user_id, guild_id)
        now = int(time.time())
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    async with db.execute('SELECT daily_reset, weekly_reset, daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_quest_completed_json, weekly_quest_completed_json, daily_stats_json, weekly_stats_json FROM users WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
                        row = await cursor.fetchone()
                    if not row:
                        await db.execute('INSERT OR IGNORE INTO users (user_id, guild_id) VALUES (?, ?)', (user_id, guild_id))
                        await db.commit()
                        return
                    daily_reset, weekly_reset, daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_completed_json, weekly_completed_json, daily_stats_json, weekly_stats_json = row
                    if daily_reset is None or daily_reset == 0 or now - int(daily_reset or 0) >= 86400:
                        daily_reset = now
                        daily_commands = 0
                        daily_reward_claimed = 0
                        daily_completed_json = '{}'
                        daily_stats_json = '{}'
                    if weekly_reset is None or weekly_reset == 0 or now - int(weekly_reset or 0) >= 604800:
                        weekly_reset = now
                        weekly_commands = 0
                        weekly_reward_claimed = 0
                        weekly_completed_json = '{}'
                        weekly_stats_json = '{}'
                    await db.execute('UPDATE users SET daily_reset = ?, weekly_reset = ?, daily_commands = ?, weekly_commands = ?, daily_reward_claimed = ?, weekly_reward_claimed = ?, daily_quest_completed_json = ?, weekly_quest_completed_json = ?, daily_stats_json = ?, weekly_stats_json = ? WHERE user_id = ? AND guild_id = ?', (daily_reset, weekly_reset, daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_completed_json, weekly_completed_json, daily_stats_json, weekly_stats_json, user_id, guild_id))
                    await db.commit()
                return
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                raise
    async def get_quest_state(self, user_id: int, guild_id: int):
        await self.ensure_quest_resets(user_id, guild_id)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    async with db.execute('SELECT daily_commands, weekly_commands, daily_quest_completed_json, weekly_quest_completed_json FROM users WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
                        row = await cursor.fetchone()
                daily_commands = int((row[0] or 0)) if row else 0
                weekly_commands = int((row[1] or 0)) if row else 0
                try:
                    daily_completed = json.loads(row[2] or '{}') if row else {}
                except:
                    daily_completed = {}
                try:
                    weekly_completed = json.loads(row[3] or '{}') if row else {}
                except:
                    weekly_completed = {}
                return {
                    "daily_commands": daily_commands,
                    "weekly_commands": weekly_commands,
                    "daily_completed": daily_completed,
                    "weekly_completed": weekly_completed
                }
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                raise
    async def claim_daily(self, user_id: int):
        data = await self.get_global_money(user_id)
        now = int(time.time())
        last = int(data['last_login'] or 0)
        streak = int(data['login_streak'] or 0)
        if last and now - last < 86400:
            remaining = 86400 - (now - last)
            return False, remaining, streak, 0
        if last and now - last <= 172800:
            streak += 1
        else:
            streak = 1
        reward = 10000 + (streak * 2000)
        tries = 4
        delay = 0.05
        for i in range(tries):
            try:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('PRAGMA busy_timeout=2000')
                    await db.execute('UPDATE users SET balance = COALESCE(balance,0) + ?, last_login = ?, login_streak = ? WHERE user_id = ? AND guild_id = 0', (reward, now, streak, user_id))
                    await db.commit()
                return True, 0, streak, reward
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                return False, 0, streak, 0
        return False, 0, streak, 0

# Blackjack Card Emojis
CARD_EMOJIS = {
    # 2s
    ('2', '♣️'): '<:2_of_clubs:1464574130169839707>',
    ('2', '♦️'): '<:2_of_diamonds:1464574132866777281>',
    ('2', '♥️'): '<:2_of_hearts:1464574134620131442>',
    ('2', '♠️'): '<:2_of_spades:1464574137111416874>',
    # 3s
    ('3', '♣️'): '<:3_of_clubs:1464574140265791692>',
    ('3', '♦️'): '<:3_of_diamonds:1464574142643699765>',
    ('3', '♥️'): '<:3_of_hearts:1464574145047036047>',
    ('3', '♠️'): '<:3_of_spades:1464574147832184862>',
    # 4s
    ('4', '♣️'): '<:4_of_clubs:1464574149660901593>',
    ('4', '♦️'): '<:4_of_diamonds:1464574151091159060>',
    ('4', '♥️'): '<:4_of_hearts:1464574158389379244>',
    ('4', '♠️'): '<:4_of_spades:1464574159949402299>',
    # 5s
    ('5', '♣️'): '<:5_of_clubs:1464574161404952576>',
    ('5', '♦️'): '<:5_of_diamonds:1464574163497783391>',
    ('5', '♥️'): '<:5_of_hearts:1464574165125169315>',
    ('5', '♠️'): '<:5_of_spades:1464574166526066769>',
    # 6s
    ('6', '♣️'): '<:6_of_clubs:1464574168585474089>',
    ('6', '♦️'): '<:6_of_diamonds:1464574171408502858>',
    ('6', '♥️'): '<:6_of_hearts:1464574173438279770>',
    ('6', '♠️'): '<:6_of_spades:1464574175678169214>',
    # 7s
    ('7', '♣️'): '<:7_of_clubs:1464574177712275466>',
    ('7', '♦️'): '<:7_of_diamonds:1464574179063103621>',
    ('7', '♥️'): '<:7_of_hearts:1464574180476321803>',
    ('7', '♠️'): '<:7_of_spades:1464574181977882634>',
    # 8s
    ('8', '♣️'): '<:8_of_clubs:1464574183852867805>',
    ('8', '♦️'): '<:8_of_diamonds:1464574185652359280>',
    ('8', '♥️'): '<:8_of_hearts:1464574187308974177>',
    ('8', '♠️'): '<:8_of_spades:1464574188848418982>',
    # 9s
    ('9', '♣️'): '<:9_of_clubs:1464574190639386736>',
    ('9', '♦️'): '<:9_of_diamonds:1464574192333885565>',
    ('9', '♥️'): '<:9_of_hearts:1464574193864540284>',
    ('9', '♠️'): '<:9_of_spades:1464574195357843539>',
    # 10s
    ('10', '♣️'): '<:10_of_clubs:1464574196762804326>',
    ('10', '♦️'): '<:10_of_diamonds:1464574198969143357>',
    ('10', '♥️'): '<:10_of_hearts:1464574200218910877>',
    ('10', '♠️'): '<:10_of_spades:1464574201661886506>',
    # Aces
    ('A', '♣️'): '<:ace_of_clubs:1464574202907459636>',
    ('A', '♦️'): '<:ace_of_diamonds:1464574204895690926>',
    ('A', '♥️'): '<:ace_of_hearts:1464574206368026769>',
    ('A', '♠️'): '<:ace_of_spades:1464574208188092466>',
    # Jacks
    ('J', '♣️'): '<:w_jack_of_clubs:1464575453888249961>',
    ('J', '♦️'): '<:w_jack_of_diamonds:1464575455305928788>',
    ('J', '♥️'): '<:w_jack_of_hearts:1464575456937513104>',
    ('J', '♠️'): '<:w_jack_of_spades:1464575460854993070>',
    # Queens
    ('Q', '♣️'): '<:w_queen_of_clubs:1464575475228872796>',
    ('Q', '♦️'): '<:w_queen_of_diamonds:1464575477057454366>',
    ('Q', '♥️'): '<:w_queen_of_hearts:1464575479779561636>',
    ('Q', '♠️'): '<:w_queen_of_spades:1464575481235116088>',
    # Kings
    ('K', '♣️'): '<:w_king_of_clubs:1464575462763266142>',
    ('K', '♦️'): '<:w_king_of_diamonds:1464575470875054259>',
    ('K', '♥️'): '<:w_king_of_hearts:1464575472745582878>',
    ('K', '♠️'): '<:w_king_of_spades:1464575473928634516>',
    # Back
    'back': '<:back:1464566298460553249>'
}

class TaskManager:
    def __init__(self):
        self.tasks: list[asyncio.Task] = []

    def register(self, coro):
        try:
            t = asyncio.create_task(coro)
            self.tasks.append(t)
        except:
            pass

    def cancel_all(self):
        for t in self.tasks:
            try:
                t.cancel()
            except:
                pass

class BossWorker:
    def __init__(self, bot, service):
        self.bot = bot
        self.service = service
        self._task = None
        self._interval = 30

    async def _loop(self):
        while True:
            try:
                if getattr(self.bot, "is_closed", lambda: True)():
                    break
                await self.service.check_and_spawn_for_all_guilds()
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break
            except:
                await asyncio.sleep(5)

    def start(self):
        try:
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._loop())
            return self._task
        except:
            return None

class AssetsService:
    async def ensure_user(self, user_id: int, guild_id: int):
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR IGNORE INTO users (user_id, guild_id) VALUES (?, ?)', (user_id, guild_id))
            await db.commit()

    async def get_guild_assets(self, guild_id: int):
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT custom_assets_json FROM guild_config WHERE guild_id = ?', (int(guild_id),)) as cursor:
                row = await cursor.fetchone()
                if row and row[0]:
                    try:
                        custom = json.loads(row[0])
                        if isinstance(custom, dict):
                            fixed = {}
                            for key, data in custom.items():
                                try:
                                    price = int(data.get("price", 0))
                                    income = int(data.get("income", 0))
                                except Exception:
                                    continue
                                if price <= 0:
                                    continue
                                if income < 0:
                                    income = 0
                                max_income = price * 20
                                if income > max_income:
                                    income = max_income
                                fixed[key] = {
                                    "name": data.get("name", key),
                                    "price": price,
                                    "income": income
                                }
                            return {**DEFAULT_ASSETS, **fixed}
                    except json.JSONDecodeError:
                        return DEFAULT_ASSETS
        return DEFAULT_ASSETS

    async def buy_asset(self, user_id: int, guild_id: int, asset_id: str, count: int):
        await self.ensure_user(user_id, guild_id)
        if count <= 0:
            return False, "Count must be positive."
        assets = await self.get_guild_assets(guild_id)
        if asset_id not in assets:
            return False, "Invalid asset ID!"
        asset = assets[asset_id]
        total_price = asset['price'] * count
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT balance FROM users WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
                row = await cursor.fetchone()
                balance = int((row[0] or 0)) if row else 0
            if balance < total_price:
                return False, f"You need **{total_price - balance:,} more coins**!"
            await db.execute('UPDATE users SET balance = MAX(0, COALESCE(balance,0) - ?) WHERE user_id = ? AND guild_id = ?', (total_price, user_id, guild_id))
            await db.execute('INSERT INTO user_assets (user_id, guild_id, asset_id, count) VALUES (?, ?, ?, ?) ON CONFLICT(user_id, guild_id, asset_id) DO UPDATE SET count = count + ?', (user_id, guild_id, asset_id, count, count))
            await db.commit()
        return True, f"✅ Bought **{count}x {asset['name']}** for **{total_price:,} coins**!"

class BossService:
    def __init__(self, bot, loot_items: dict, bosses: list[dict]):
        self.bot = bot
        self.loot_items = loot_items
        self.bosses = bosses

    def hp_bar(self, hp: int, max_hp: int, length: int = 20) -> str:
        pct = 0 if max_hp <= 0 else max(0, min(1, hp / max_hp))
        filled = int(length * pct)
        return "█" * filled + "░" * (length - filled)

    async def get_interval(self, guild_id: int) -> int:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT boss_spawn_interval FROM guild_config WHERE guild_id = ?', (guild_id,)) as c:
                row = await c.fetchone()
                return int(row[0]) if row and row[0] else 3600

    async def get_channel(self, guild: discord.Guild) -> discord.TextChannel | None:
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                async with db.execute('SELECT boss_channel_id FROM guild_config WHERE guild_id = ?', (guild.id,)) as c:
                    row = await c.fetchone()
            if row and row[0]:
                ch = guild.get_channel(int(row[0]))
                if ch:
                    me = guild.me or guild.get_member(self.bot.user.id)
                    if me and ch.permissions_for(me).send_messages:
                        return ch
        except:
            pass
        return None

    def pick_text_channel(self, guild: discord.Guild) -> discord.TextChannel | None:
        for ch in guild.text_channels:
            try:
                me = guild.me or guild.get_member(self.bot.user.id)
                if me and ch.permissions_for(me).send_messages:
                    return ch
            except:
                continue
        return None

    async def spawn_boss(self, guild: discord.Guild):
        if not guild:
            return
        boss = random.choice(self.bosses)
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR REPLACE INTO boss_state (guild_id, boss_name, difficulty, max_hp, hp, spawned_at, is_active) VALUES (?, ?, ?, ?, ?, ?, 1)',
                             (guild.id, boss["name"], boss["difficulty"], boss["hp"], boss["hp"], int(time.time())))
            await db.execute('DELETE FROM boss_damage WHERE guild_id = ?', (guild.id,))
            await db.commit()
        ch = await self.get_channel(guild) or self.pick_text_channel(guild)
        if ch:
            embed = discord.Embed(title=f"👹 Boss Spawned — {boss['name']}", color=discord.Color.red(), timestamp=discord.utils.utcnow())
            embed.add_field(name="Difficulty", value=boss["difficulty"], inline=True)
            embed.add_field(name="HP", value=f"{boss['hp']:,}\n{self.hp_bar(boss['hp'], boss['hp'])}", inline=True)
            try:
                await ch.send(embed=embed)
            except:
                pass

    async def get_boss(self, guild_id: int):
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('SELECT * FROM boss_state WHERE guild_id = ?', (guild_id,)) as c:
                return await c.fetchone()

    async def set_boss_hp(self, guild_id: int, new_hp: int):
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('UPDATE boss_state SET hp = ?, is_active = CASE WHEN ? <= 0 THEN 0 ELSE 1 END WHERE guild_id = ?', (max(0, new_hp), new_hp, guild_id))
            await db.commit()

    async def add_damage(self, guild_id: int, user_id: int, dmg: int):
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT INTO boss_damage (guild_id, user_id, damage) VALUES (?, ?, ?) ON CONFLICT(guild_id, user_id) DO UPDATE SET damage = damage + ?', (guild_id, user_id, dmg, dmg))
            await db.commit()

    def rarity_chance(self, rarity: str) -> float:
        return {"common": 0.6, "uncommon": 0.4, "rare": 0.25, "epic": 0.10, "legendary": 0.04}.get(rarity, 0.2)

    async def distribute_loot(self, guild: discord.Guild):
        boss = await self.get_boss(guild.id)
        if not boss:
            return
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT SUM(damage) FROM boss_damage WHERE guild_id = ?', (guild.id,)) as c:
                total = (await c.fetchone())[0] or 0
            async with db.execute('SELECT user_id, damage FROM boss_damage WHERE guild_id = ?', (guild.id,)) as c:
                rows = await c.fetchall()
        if total <= 0 or not rows:
            return
        lootable_ids = next((b["loot"] for b in BOSSES if b["name"] == boss["boss_name"]), [])
        for uid, dmg in rows:
            share = (dmg or 0) / (total or 1)
            drop_pool = []
            for iid in lootable_ids:
                item = LOOT_ITEMS[iid]
                chance = self.rarity_chance(item["rarity"]) * share
                if chance >= 0.9 or random.random() < chance:
                    drop_pool.append(iid)
            awarded = drop_pool or ([random.choice(lootable_ids)] if lootable_ids else [])
            for iid in awarded:
                item = LOOT_ITEMS[iid]
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute('INSERT INTO boss_items (user_id, item_id, count) VALUES (?, ?, 1) ON CONFLICT(user_id, item_id) DO UPDATE SET count = count + 1', (uid, iid))
                    await db.commit()
            member = guild.get_member(uid)
            if member:
                try:
                    await member.send(f"🎁 Boss defeated in {guild.name}! You received: " + ", ".join([LOOT_ITEMS[i]['name'] for i in awarded]))
                except:
                    pass

    async def check_and_spawn_for_all_guilds(self):
        try:
            now = int(time.time())
            for g in self.bot.guilds:
                boss = await self.get_boss(g.id)
                interval = await self.get_interval(g.id)
                if not boss or not boss["is_active"] or (boss["spawned_at"] or 0) + interval <= now:
                    await self.spawn_boss(g)
        except:
            pass

def setup_assets_commands(bot):
    assets = AssetsService()
    @bot.hybrid_command(name="shop", description="View the asset shop")
    async def shop(ctx: commands.Context):
        config = await assets.get_guild_assets(ctx.guild.id)
        embed = discord.Embed(title="🛒 Kingdom Asset Shop", description="Buy assets to earn passive income every 10 minutes!", color=0x00d2ff)
        for aid, data in config.items():
            embed.add_field(name=f"{data['name']} (ID: {aid})", value=f"Price: 🪙 {data['price']:,}\nIncome: 💸 {data['income']:,}/10min", inline=False)
        await ctx.send(embed=embed)
    @bot.hybrid_command(name="buy", description="Buy a passive income asset")
    async def buy_asset(ctx: commands.Context, asset_id: str, count: int = 1):
        ok, msg = await assets.buy_asset(ctx.author.id, ctx.guild.id, asset_id, count)
        await ctx.send(msg)

async def init_db():
    async with aiosqlite.connect(DB_FILE, timeout=30) as db:
        try:
            await db.execute('PRAGMA journal_mode=WAL')
        except:
            pass
        await db.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER, guild_id INTEGER, balance INTEGER DEFAULT 100,
            bank INTEGER DEFAULT 0, xp INTEGER DEFAULT 0, level INTEGER DEFAULT 1,
            prestige INTEGER DEFAULT 0, last_work INTEGER DEFAULT 0,
            last_crime INTEGER DEFAULT 0, last_rob INTEGER DEFAULT 0,
            last_vote INTEGER DEFAULT 0, auto_deposit INTEGER DEFAULT 0,
            bank_plan TEXT DEFAULT 'standard',
            daily_commands INTEGER DEFAULT 0, daily_reset INTEGER DEFAULT 0,
            daily_reward_claimed INTEGER DEFAULT 0,
            weekly_commands INTEGER DEFAULT 0, weekly_reset INTEGER DEFAULT 0,
            weekly_reward_claimed INTEGER DEFAULT 0,
            daily_quest_completed_json TEXT DEFAULT '{}',
            weekly_quest_completed_json TEXT DEFAULT '{}',
            daily_stats_json TEXT DEFAULT '{}',
            weekly_stats_json TEXT DEFAULT '{}',
            PRIMARY KEY (user_id, guild_id)
        )''')
        try:
            cols = []
            async with db.execute("PRAGMA table_info(users)") as c:
                rows = await c.fetchall()
                cols = [r[1] for r in rows] if rows else []
            req = {
                "xp": "INTEGER DEFAULT 0",
                "level": "INTEGER DEFAULT 1",
                "prestige": "INTEGER DEFAULT 0",
                "last_work": "INTEGER DEFAULT 0",
                "last_crime": "INTEGER DEFAULT 0",
                "last_rob": "INTEGER DEFAULT 0",
                "last_vote": "INTEGER DEFAULT 0",
                "auto_deposit": "INTEGER DEFAULT 0",
                "bank_plan": "TEXT DEFAULT 'standard'",
                "daily_commands": "INTEGER DEFAULT 0",
                "daily_reset": "INTEGER DEFAULT 0",
                "daily_reward_claimed": "INTEGER DEFAULT 0",
                "weekly_commands": "INTEGER DEFAULT 0",
                "weekly_reset": "INTEGER DEFAULT 0",
                "weekly_reward_claimed": "INTEGER DEFAULT 0",
                "daily_quest_completed_json": "TEXT DEFAULT '{}'",
                "weekly_quest_completed_json": "TEXT DEFAULT '{}'",
                "daily_stats_json": "TEXT DEFAULT '{}'",
                "weekly_stats_json": "TEXT DEFAULT '{}'",
                "started": "INTEGER DEFAULT 0"
            }
            for name, decl in req.items():
                if name not in cols:
                    try:
                        await db.execute(f"ALTER TABLE users ADD COLUMN {name} {decl}")
                    except:
                        pass
        except:
            pass
        try:
            await db.execute('ALTER TABLE users ADD COLUMN last_vote INTEGER DEFAULT 0')
            await db.execute('ALTER TABLE users ADD COLUMN auto_deposit INTEGER DEFAULT 0')
        except:
            pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN bank_plan TEXT DEFAULT 'standard'")
        except:
            pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN daily_commands INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE users ADD COLUMN daily_reset INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE users ADD COLUMN daily_reward_claimed INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE users ADD COLUMN weekly_commands INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE users ADD COLUMN weekly_reset INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE users ADD COLUMN weekly_reward_claimed INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN daily_quest_completed_json TEXT DEFAULT '{}'")
            await db.execute("ALTER TABLE users ADD COLUMN weekly_quest_completed_json TEXT DEFAULT '{}'")
            await db.execute("ALTER TABLE users ADD COLUMN daily_stats_json TEXT DEFAULT '{}'")
            await db.execute("ALTER TABLE users ADD COLUMN weekly_stats_json TEXT DEFAULT '{}'")
        except:
            pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN started INTEGER DEFAULT 0")
        except:
            pass
            
        await db.execute('''CREATE TABLE IF NOT EXISTS user_assets (
            user_id INTEGER, guild_id INTEGER, asset_id TEXT, count INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, guild_id, asset_id)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS marriages (
            user_id INTEGER PRIMARY KEY,
            partner_id INTEGER
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS guild_config (
            guild_id INTEGER PRIMARY KEY, prefix TEXT DEFAULT '.',
            role_shop_json TEXT DEFAULT '{}', custom_assets_json TEXT DEFAULT '{}',
            bank_plans_json TEXT DEFAULT '{}'
        )''')
        try:
            await db.execute("ALTER TABLE guild_config ADD COLUMN bank_plans_json TEXT DEFAULT '{}'")
        except:
            pass
        await db.execute('''CREATE TABLE IF NOT EXISTS guild_wonder (
            guild_id INTEGER PRIMARY KEY,
            level INTEGER DEFAULT 0,
            progress INTEGER DEFAULT 0,
            goal INTEGER DEFAULT 50000,
            boost_multiplier REAL DEFAULT 1.25,
            boost_until INTEGER DEFAULT 0
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS user_jobs (
            user_id INTEGER, guild_id INTEGER, job_id TEXT,
            PRIMARY KEY (user_id, guild_id)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS global_votes (
            user_id INTEGER PRIMARY KEY, last_vote INTEGER DEFAULT 0
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS user_rewards (
            user_id INTEGER PRIMARY KEY,
            multipliers_json TEXT DEFAULT '{}',
            titles_json TEXT DEFAULT '[]',
            medals_json TEXT DEFAULT '[]'
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS title_templates (
            title_name TEXT PRIMARY KEY,
            description TEXT,
            created_at INTEGER
        )''')

        # --- MODERATION & UTILITY TABLES ---
        await db.execute('''CREATE TABLE IF NOT EXISTS warnings (
            warn_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            guild_id INTEGER,
            moderator_id INTEGER,
            reason TEXT,
            timestamp INTEGER,
            expires_at INTEGER
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS automod_words (
            word_id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            word TEXT,
            punishment TEXT DEFAULT 'warn'
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS logging_config (
            guild_id INTEGER PRIMARY KEY,
            message_log_channel INTEGER,
            member_log_channel INTEGER,
            join_log_channel INTEGER,
            leave_log_channel INTEGER,
            user_log_channel INTEGER,
            server_log_channel INTEGER,
            voice_log_channel INTEGER,
            mod_log_channel INTEGER,
            automod_log_channel INTEGER,
            command_log_channel INTEGER
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS welcome_farewell (
            guild_id INTEGER PRIMARY KEY,
            welcome_channel INTEGER,
            welcome_message TEXT,
            welcome_embed_json TEXT,
            farewell_channel INTEGER,
            farewell_message TEXT,
            farewell_embed_json TEXT
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS reaction_roles (
            message_id INTEGER,
            guild_id INTEGER,
            emoji TEXT,
            role_id INTEGER,
            PRIMARY KEY (message_id, emoji)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS custom_commands (
            guild_id INTEGER,
            name TEXT,
            code TEXT,
            prefix TEXT DEFAULT '.',
            PRIMARY KEY (guild_id, name)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS alliances (
            alliance_id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            name TEXT UNIQUE,
            bank INTEGER DEFAULT 0,
            owner_id INTEGER
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS alliance_members (
            alliance_id INTEGER,
            user_id INTEGER,
            role TEXT DEFAULT 'member',
            PRIMARY KEY (alliance_id, user_id)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS vassals (
            lord_id INTEGER,
            vassal_id INTEGER,
            guild_id INTEGER,
            percent INTEGER DEFAULT 5,
            PRIMARY KEY (lord_id, vassal_id, guild_id)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS market_listings (
            listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            seller_id INTEGER,
            item TEXT,
            price INTEGER,
            quantity INTEGER DEFAULT 1,
            created_at INTEGER
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS marriage_proposals (
            proposer_id INTEGER,
            target_id INTEGER,
            guild_id INTEGER,
            created_at INTEGER,
            PRIMARY KEY (proposer_id, target_id, guild_id)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS divorce_cases (
            case_id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            spouse1_id INTEGER,
            spouse2_id INTEGER,
            kids INTEGER DEFAULT 0,
            questions_json TEXT,
            answers1_json TEXT,
            answers2_json TEXT,
            status TEXT DEFAULT 'pending',
            fines_json TEXT
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS mod_stats (
            user_id INTEGER,
            guild_id INTEGER,
            messages INTEGER DEFAULT 0,
            warns INTEGER DEFAULT 0,
            bans INTEGER DEFAULT 0,
            kicks INTEGER DEFAULT 0,
            timeouts INTEGER DEFAULT 0,
            points INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, guild_id)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS bot_guilds (
            guild_id INTEGER PRIMARY KEY,
            first_seen INTEGER
        )''')
        await db.commit()

async def migrate_db():
    async with aiosqlite.connect(DB_FILE) as db:
        # Columns to add to users table
        columns = [
            ("total_commands", "INTEGER DEFAULT 0"),
            ("successful_robs", "INTEGER DEFAULT 0"),
            ("successful_crimes", "INTEGER DEFAULT 0"),
            ("passive_income", "REAL DEFAULT 0.0"),
            ("blackjack_wins", "INTEGER DEFAULT 0"),
            ("last_login", "INTEGER DEFAULT 0"),
            ("login_streak", "INTEGER DEFAULT 0")
        ]
        for col_name, col_type in columns:
            try:
                await db.execute(f'ALTER TABLE users ADD COLUMN {col_name} {col_type}')
            except:
                pass
        try:
            await db.execute('ALTER TABLE logging_config ADD COLUMN join_log_channel INTEGER')
        except:
            pass
        try:
            await db.execute('ALTER TABLE logging_config ADD COLUMN leave_log_channel INTEGER')
        except:
            pass
        try:
            await db.execute('ALTER TABLE logging_config ADD COLUMN use_webhooks INTEGER DEFAULT 0')
        except:
            pass
        try:
            await db.execute("ALTER TABLE guild_config ADD COLUMN raid_mode INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE guild_config ADD COLUMN anti_phish_enabled INTEGER DEFAULT 1")
        except:
            pass
        try:
            await db.execute("ALTER TABLE guild_config ADD COLUMN marketplace_enabled INTEGER DEFAULT 1")
        except:
            pass
        try:
            await db.execute("ALTER TABLE guild_config ADD COLUMN marketplace_tax INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE guild_config ADD COLUMN vassal_max_percent INTEGER DEFAULT 15")
        except:
            pass
        try:
            await db.execute("ALTER TABLE guild_config ADD COLUMN alliances_enabled INTEGER DEFAULT 1")
        except:
            pass
        try:
            await db.execute("ALTER TABLE marriages ADD COLUMN kids INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS owner_access (
                guild_id INTEGER,
                user_id INTEGER,
                PRIMARY KEY (guild_id, user_id)
            )''')
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS abuse_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER,
                reporter_id INTEGER,
                accused_id INTEGER,
                reason TEXT,
                evidence TEXT,
                status TEXT DEFAULT 'pending',
                created_at INTEGER
            )''')
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS raid_state (
                guild_id INTEGER PRIMARY KEY,
                started_at INTEGER,
                duration_sec INTEGER,
                restore_sec INTEGER,
                lock_active INTEGER DEFAULT 0,
                channel_overwrites_json TEXT DEFAULT '{}',
                timeouts_json TEXT DEFAULT '[]'
            )''')
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS process_lock (
                id INTEGER PRIMARY KEY CHECK (id=1),
                holder TEXT,
                expires INTEGER
            )''')
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS boss_state (
                guild_id INTEGER PRIMARY KEY,
                boss_name TEXT,
                difficulty TEXT,
                max_hp INTEGER,
                hp INTEGER,
                spawned_at INTEGER,
                is_active INTEGER DEFAULT 0
            )''')
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS boss_damage (
                guild_id INTEGER,
                user_id INTEGER,
                damage INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            )''')
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS boss_items (
                user_id INTEGER,
                item_id TEXT,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, item_id)
            )''')
        except:
            pass
        try:
            await db.execute('ALTER TABLE guild_config ADD COLUMN boss_spawn_interval INTEGER DEFAULT 3600')
        except:
            pass
        try:
            await db.execute('ALTER TABLE guild_config ADD COLUMN boss_channel_id INTEGER')
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS auto_sync_config (
                id INTEGER PRIMARY KEY CHECK (id=1),
                enabled INTEGER DEFAULT 0,
                last_run INTEGER DEFAULT 0
            )''')
        except:
            pass
        try:
            await db.execute('''CREATE TABLE IF NOT EXISTS guild_auto_role (
                guild_id INTEGER PRIMARY KEY,
                role_id INTEGER
            )''')
        except:
            pass
        await db.commit()

@tasks.loop(seconds=60)
async def instance_heartbeat_task():
    if not INSTANCE_ID:
        return
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('UPDATE bot_instances SET updated_at = ? WHERE inst_id = ?', (int(time.time()), INSTANCE_ID))
            await db.execute('UPDATE process_lock SET expires = ? WHERE id = 1 AND holder = ?', (int(time.time()) + 120, INSTANCE_ID))
            await db.commit()
    except:
        pass
    try:
        if os.path.exists(FILE_LOCK_PATH):
            os.utime(FILE_LOCK_PATH, None)
    except:
        pass

async def ensure_single_instance():
    global INSTANCE_ID
    if not INSTANCE_ID:
        INSTANCE_ID = f"{os.uname().nodename}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    now = int(time.time())
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS bot_instances (
                inst_id TEXT PRIMARY KEY,
                updated_at INTEGER,
                hostname TEXT
            )''')
            await db.execute('DELETE FROM bot_instances WHERE updated_at < ?', (now - 3600,))
            # DB singleton lock using UPSERT only if expired
            await db.execute('''CREATE TABLE IF NOT EXISTS process_lock (
                id INTEGER PRIMARY KEY CHECK (id=1),
                holder TEXT,
                expires INTEGER
            )''')
            await db.execute('''INSERT INTO process_lock (id, holder, expires) VALUES (1, ?, ?)
                                ON CONFLICT(id) DO UPDATE SET holder=excluded.holder, expires=excluded.expires
                                WHERE process_lock.expires < ?''', (INSTANCE_ID, now + 120, now))
            async with db.execute('SELECT holder, expires FROM process_lock WHERE id = 1') as c:
                row = await c.fetchone()
            if not row:
                return True
            holder, expires = row
            if holder != INSTANCE_ID and (expires or 0) >= now:
                return False
            await db.execute('INSERT OR REPLACE INTO bot_instances (inst_id, updated_at, hostname) VALUES (?, ?, ?)', (INSTANCE_ID, now, os.uname().nodename))
            await db.commit()
        return True
    except:
        return True
def _acquire_file_lock():
    try:
        if os.path.exists(FILE_LOCK_PATH):
            mtime = os.path.getmtime(FILE_LOCK_PATH)
            if time.time() - mtime > 180:
                try:
                    os.remove(FILE_LOCK_PATH)
                except:
                    pass
        fd = os.open(FILE_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(f"{os.uname().nodename}:{os.getpid()}:{INSTANCE_ID}:{int(time.time())}\n")
        return True
    except FileExistsError:
        return False
    except:
        return True
def _release_file_lock():
    try:
        if os.path.exists(FILE_LOCK_PATH):
            os.remove(FILE_LOCK_PATH)
    except:
        pass

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

# Cache for guild prefixes to optimize performance
PREFIX_CACHE = {}

async def get_prefix(bot, message):
    if not message.guild: return '.'
    guild_id = message.guild.id
    if guild_id in PREFIX_CACHE:
        return PREFIX_CACHE[guild_id]
    tries = 4
    delay = 0.05
    async with aiosqlite.connect(DB_FILE) as db:
        try:
            await db.execute('PRAGMA journal_mode=WAL')
            await db.execute('PRAGMA busy_timeout=2000')
        except:
            pass
        for i in range(tries):
            try:
                async with db.execute('SELECT prefix FROM guild_config WHERE guild_id = ?', (guild_id,)) as cursor:
                    row = await cursor.fetchone()
                    prefix = row[0] if row else '.'
                    PREFIX_CACHE[guild_id] = prefix
                    return prefix
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                break
    return PREFIX_CACHE.get(guild_id, '.')
intents.members = True
intents.message_content = True 
bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)

# Safety: avoid dispatching events when loop/client is closing
_orig_dispatch = bot.dispatch
def _safe_dispatch(event_name, *args, **kwargs):
    try:
        if bot.is_closed():
            return
        lp = getattr(bot, "loop", None)
        if lp is None or lp.is_closed():
            return
    except:
        return
    return _orig_dispatch(event_name, *args, **kwargs)
bot.dispatch = _safe_dispatch

@bot.before_invoke
async def _auto_defer(ctx):
    try:
        if ctx.interaction and not ctx.interaction.response.is_done():
            await ctx.interaction.response.defer(ephemeral=False)
    except:
        pass

# Debug: Check if token is loaded
if not TOKEN:
    print("CRITICAL: DISCORD_TOKEN not found in .env file!")
else:
    TOKEN = TOKEN.strip()

# --- Database Helpers ---
async def ensure_rewards(user_id, db=None):
    async def _insert(conn):
        tries = 5
        delay = 0.05
        for i in range(tries):
            try:
                await conn.execute('INSERT OR IGNORE INTO user_rewards (user_id) VALUES (?)', (user_id,))
                return True
            except Exception as e:
                if "database is locked" in str(e).lower():
                    await asyncio.sleep(delay * (i + 1))
                    continue
                raise
        return False
    if db is not None:
        ok = await _insert(db)
        return ok
    async with aiosqlite.connect(DB_FILE) as db_local:
        try:
            await db_local.execute('PRAGMA journal_mode=WAL')
            await db_local.execute('PRAGMA busy_timeout=5000')
        except:
            pass
        await _insert(db_local)
        await db_local.commit()

async def get_user_multipliers(user_id):
    await ensure_rewards(user_id)
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT multipliers_json FROM user_rewards WHERE user_id = ?', (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                return json.loads(row[0])
    return {}

async def get_total_multiplier(user_id):
    multipliers = await get_user_multipliers(user_id)
    total = 1.0
    for m in multipliers.values():
        total += (m - 1.0)
    return max(1.0, total)

# --- Boss System Helpers & Tasks ---
def _hp_bar(hp: int, max_hp: int, length: int = 20) -> str:
    pct = 0 if max_hp <= 0 else max(0, min(1, hp / max_hp))
    filled = int(length * pct)
    return "█" * filled + "░" * (length - filled)

async def _get_boss_interval(guild_id: int) -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT boss_spawn_interval FROM guild_config WHERE guild_id = ?', (guild_id,)) as c:
            row = await c.fetchone()
            return int(row[0]) if row and row[0] else 3600

async def _get_boss_channel(guild: discord.Guild) -> discord.TextChannel | None:
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT boss_channel_id FROM guild_config WHERE guild_id = ?', (guild.id,)) as c:
                row = await c.fetchone()
        if row and row[0]:
            ch = guild.get_channel(int(row[0]))
            if ch:
                me = guild.me or guild.get_member(bot.user.id)
                if me and ch.permissions_for(me).send_messages:
                    return ch
    except:
        pass
    return None

def _pick_text_channel(guild: discord.Guild) -> discord.TextChannel | None:
    for ch in getattr(guild, "text_channels", []):
        me = guild.me or guild.get_member(bot.user.id)
        if not me: return ch
        try:
            if ch.permissions_for(me).send_messages:
                return ch
        except:
            continue
    return None

async def _spawn_boss(guild: discord.Guild):
    if not guild:
        return
    boss = random.choice(BOSSES)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR REPLACE INTO boss_state (guild_id, boss_name, difficulty, max_hp, hp, spawned_at, is_active) VALUES (?, ?, ?, ?, ?, ?, 1)',
                         (guild.id, boss["name"], boss["difficulty"], boss["hp"], boss["hp"], int(time.time())))
        await db.execute('DELETE FROM boss_damage WHERE guild_id = ?', (guild.id,))
        await db.commit()
    ch = await _get_boss_channel(guild) or _pick_text_channel(guild)
    if ch:
        embed = discord.Embed(title=f"👹 Boss Spawned — {boss['name']}", color=discord.Color.red(), timestamp=discord.utils.utcnow())
        embed.add_field(name="Difficulty", value=boss["difficulty"], inline=True)
        embed.add_field(name="HP", value=f"{boss['hp']:,}\n{_hp_bar(boss['hp'], boss['hp'])}", inline=True)
        try:
            await ch.send(embed=embed)
        except:
            pass

async def _get_boss(guild_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM boss_state WHERE guild_id = ?', (guild_id,)) as c:
            return await c.fetchone()

async def _set_boss_hp(guild_id: int, new_hp: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('UPDATE boss_state SET hp = ?, is_active = CASE WHEN ? <= 0 THEN 0 ELSE 1 END WHERE guild_id = ?', (max(0, new_hp), new_hp, guild_id))
        await db.commit()

async def _add_damage(guild_id: int, user_id: int, dmg: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT INTO boss_damage (guild_id, user_id, damage) VALUES (?, ?, ?) ON CONFLICT(guild_id, user_id) DO UPDATE SET damage = damage + ?', (guild_id, user_id, dmg, dmg))
        await db.commit()

def _rarity_chance(rarity: str) -> float:
    return {"common": 0.6, "uncommon": 0.4, "rare": 0.25, "epic": 0.10, "legendary": 0.04}.get(rarity, 0.2)

async def _distribute_loot(guild: discord.Guild):
    boss = await _get_boss(guild.id)
    if not boss:
        return
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT SUM(damage) FROM boss_damage WHERE guild_id = ?', (guild.id,)) as c:
            total = (await c.fetchone())[0] or 0
        async with db.execute('SELECT user_id, damage FROM boss_damage WHERE guild_id = ?', (guild.id,)) as c:
            rows = await c.fetchall()
    if total <= 0 or not rows:
        return
    lootable_ids = next((b["loot"] for b in BOSSES if b["name"] == boss["boss_name"]), [])
    for uid, dmg in rows:
        share = dmg / total
        drop_pool = []
        for iid in lootable_ids:
            item = LOOT_ITEMS[iid]
            chance = _rarity_chance(item["rarity"]) * share
            if chance >= 0.9 or random.random() < chance:
                drop_pool.append(iid)
        awarded = drop_pool or ([random.choice(lootable_ids)] if lootable_ids else [])
        for iid in awarded:
            item = LOOT_ITEMS[iid]
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('INSERT INTO boss_items (user_id, item_id, count) VALUES (?, ?, 1) ON CONFLICT(user_id, item_id) DO UPDATE SET count = count + 1', (uid, iid))
                if item["effect"] in ("wallet_add", "sellable"):
                    await update_global_balance(uid, item["value"])
                await db.commit()
        member = guild.get_member(uid)
        if member:
            try:
                await member.send(f"🎁 Boss defeated in {guild.name}! You received: " + ", ".join([LOOT_ITEMS[i]['name'] for i in awarded]))
            except:
                pass

 

async def _clear_guild_commands_once():
    try:
        for g in bot.guilds:
            try:
                bot.tree.clear_commands(guild=g)
                await bot.tree.sync(guild=g)
            except:
                pass
    except:
        pass
async def ensure_user(user_id, guild_id):
    tries = 4
    delay = 0.05
    for i in range(tries):
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('PRAGMA busy_timeout=2000')
                await db.execute('INSERT OR IGNORE INTO users (user_id, guild_id) VALUES (?, ?)', (user_id, guild_id))
                await db.commit()
            return
        except Exception as e:
            if "database is locked" in str(e).lower():
                await asyncio.sleep(delay * (i + 1))
                continue
            raise

async def ensure_global_user(user_id):
    await ensure_user(user_id, 0)

async def get_global_money(user_id):
    await ensure_global_user(user_id)
    tries = 4
    delay = 0.05
    for i in range(tries):
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('PRAGMA busy_timeout=2000')
                db.row_factory = aiosqlite.Row
                async with db.execute('SELECT user_id, guild_id, balance, bank, bank_plan, last_work, last_crime, last_rob FROM users WHERE user_id = ? AND guild_id = 0', (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return row
                async with db.execute('SELECT COALESCE(SUM(balance),0), COALESCE(SUM(bank),0) FROM users WHERE user_id = ?', (user_id,)) as cursor:
                    agg = await cursor.fetchone()
                balance_sum = int((agg[0] or 0))
                bank_sum = int((agg[1] or 0))
                await db.execute('INSERT OR REPLACE INTO users (user_id, guild_id, balance, bank, bank_plan) VALUES (?, 0, ?, ?, ?)', (user_id, balance_sum, bank_sum, 'standard'))
                await db.commit()
            async with aiosqlite.connect(DB_FILE) as db2:
                await db2.execute('PRAGMA busy_timeout=2000')
                db2.row_factory = aiosqlite.Row
                async with db2.execute('SELECT user_id, guild_id, balance, bank, bank_plan, last_work, last_crime, last_rob FROM users WHERE user_id = ? AND guild_id = 0', (user_id,)) as cursor2:
                    return await cursor2.fetchone()
        except Exception as e:
            if "database is locked" in str(e).lower():
                await asyncio.sleep(delay * (i + 1))
                continue
            raise

async def update_global_balance(user_id, delta):
    await ensure_global_user(user_id)
    tries = 4
    delay = 0.05
    for i in range(tries):
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('PRAGMA busy_timeout=2000')
                await db.execute('UPDATE users SET balance = MAX(0, balance + ?) WHERE user_id = ? AND guild_id = 0', (int(delta), user_id))
                await db.commit()
            return
        except Exception as e:
            if "database is locked" in str(e).lower():
                await asyncio.sleep(delay * (i + 1))
                continue
            raise

async def move_global_wallet_to_bank(user_id, amount):
    await ensure_global_user(user_id)
    amt = int(amount)
    tries = 4
    delay = 0.05
    for i in range(tries):
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('PRAGMA busy_timeout=2000')
                await db.execute('UPDATE users SET balance = MAX(0, COALESCE(balance,0) - ?), bank = COALESCE(bank,0) + ? WHERE user_id = ? AND guild_id = 0', (amt, amt, user_id))
                await db.commit()
            return
        except Exception as e:
            if "database is locked" in str(e).lower():
                await asyncio.sleep(delay * (i + 1))
                continue
            raise

async def move_global_bank_to_wallet(user_id, amount):
    await ensure_global_user(user_id)
    amt = int(amount)
    tries = 4
    delay = 0.05
    for i in range(tries):
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('PRAGMA busy_timeout=2000')
                await db.execute('UPDATE users SET bank = MAX(0, COALESCE(bank,0) - ?), balance = COALESCE(balance,0) + ? WHERE user_id = ? AND guild_id = 0', (amt, amt, user_id))
                await db.commit()
            return
        except Exception as e:
            if "database is locked" in str(e).lower():
                await asyncio.sleep(delay * (i + 1))
                continue
            raise

async def has_owner_access(guild_id, user_id):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT 1 FROM owner_access WHERE guild_id = ? AND user_id = ?', (guild_id, user_id)) as c:
            return (await c.fetchone()) is not None

def is_guild_owner_only():
    async def predicate(ctx):
        return ctx.guild and ctx.author.id == ctx.guild.owner_id
    return commands.check(predicate)

def is_owner_or_delegate():
    async def predicate(ctx):
        if not ctx.guild:
            return False
        if ctx.author.id in BOT_OWNERS:
            return True
        if ctx.author.id == ctx.guild.owner_id:
            return True
        return await has_owner_access(ctx.guild.id, ctx.author.id)
    return commands.check(predicate)

def is_authorized_owner():
    return is_owner_or_delegate()

def owner_or_has(**required):
    async def predicate(ctx):
        if ctx.guild:
            if ctx.author.id in BOT_OWNERS:
                return True
            if ctx.author.id == ctx.guild.owner_id:
                return True
            if await has_owner_access(ctx.guild.id, ctx.author.id):
                return True
            gp = ctx.author.guild_permissions
            for k, v in required.items():
                if v and not getattr(gp, k, False):
                    return False
            return True
        return True
    return commands.check(predicate)

def owner_or_admin():
    return owner_or_has(administrator=True)
async def _get_head_admin_role_id(guild_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT tier_head_admin_role_id FROM promo_config WHERE guild_id = ?', (guild_id,)) as c:
            row = await c.fetchone()
        if not row:
            return None
        try:
            rid = int(row[0]) if row[0] is not None else None
        except:
            rid = None
        return rid

def is_head_admin_only():
    async def predicate(ctx):
        if not ctx.guild:
            return False
        rid = await _get_head_admin_role_id(ctx.guild.id)
        if not rid:
            return False
        return any(r.id == rid for r in getattr(ctx.author, "roles", []))
    return commands.check(predicate)

async def _create_invite_for_guild(guild: discord.Guild):
    me = guild.me or guild.get_member(bot.user.id)
    if not me:
        return None
    for ch in list(getattr(guild, "text_channels", []))[:3]:
        try:
            perms = ch.permissions_for(me)
            if perms.create_instant_invite:
                try:
                    inv = await asyncio.wait_for(ch.create_invite(max_age=3600, max_uses=1, unique=True), timeout=2.0)
                except Exception:
                    inv = None
                return getattr(inv, "url", None)
        except:
            pass
    return None
async def add_xp(user_id, guild_id, amount):
    await ensure_user(user_id, guild_id)
    tries = 4
    delay = 0.05
    for i in range(tries):
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('PRAGMA busy_timeout=2000')
                await db.execute('UPDATE users SET xp = xp + ? WHERE user_id = ? AND guild_id = ?', (amount, user_id, guild_id))
                await db.commit()
                async with db.execute('SELECT xp, level FROM users WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        current_xp, current_level = row
                        next_level_xp = current_level * 100
                        if current_xp >= next_level_xp:
                            new_level = current_level + 1
                            await db.execute('UPDATE users SET level = ?, xp = xp - ? WHERE user_id = ? AND guild_id = ?', (new_level, next_level_xp, user_id, guild_id))
                            await db.commit()
                            return True, new_level
            break
        except Exception as e:
            if "database is locked" in str(e).lower():
                await asyncio.sleep(delay * (i + 1))
                continue
            raise
    return False, None

async def get_user_data(user_id, guild_id):
    await ensure_user(user_id, guild_id)
    tries = 4
    delay = 0.05
    for i in range(tries):
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('PRAGMA busy_timeout=2000')
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT u.*, 
                           CASE 
                               WHEN COALESCE(gv.last_vote, 0) > COALESCE(u.last_vote, 0) 
                               THEN gv.last_vote 
                               ELSE COALESCE(u.last_vote, 0) 
                           END as last_vote
                    FROM users u
                    LEFT JOIN global_votes gv ON u.user_id = gv.user_id
                    WHERE u.user_id = ? AND u.guild_id = ?
                ''', (user_id, guild_id)) as cursor:
                    row = await cursor.fetchone()
                    return row
        except Exception as e:
            if "database is locked" in str(e).lower():
                await asyncio.sleep(delay * (i + 1))
                continue
            raise

# --- MODERATION HELPERS ---

async def log_embed(guild, config_key, embed):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(f'SELECT {config_key} FROM logging_config WHERE guild_id = ?', (guild.id,)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                channel_id = row[0]
                try:
                    channel_id = int(channel_id)
                except:
                    pass
                channel = guild.get_channel(channel_id)
                if channel:
                    try:
                        await channel.send(embed=apply_theme(embed))
                    except:
                        pass

async def _find_actor(guild: discord.Guild, action: discord.AuditLogAction, target_id: int):
    user = None
    reason = None
    try:
        await asyncio.sleep(1)
        async for entry in guild.audit_logs(limit=6, action=action):
            tgt = entry.target
            if hasattr(tgt, "id") and tgt.id == target_id:
                user = entry.user
                reason = entry.reason
                break
    except:
        pass
    return user, reason

@bot.event
async def on_message_delete(message):
    if not message.guild or message.author.bot:
        return
    embed = discord.Embed(title="🗑️ Message Deleted", color=discord.Color.orange(), timestamp=discord.utils.utcnow())
    embed.add_field(name="Author", value=f"{message.author.mention} ({message.author.id})", inline=True)
    embed.add_field(name="Channel", value=message.channel.mention, inline=True)
    embed.add_field(name="Message ID", value=str(message.id), inline=True)
    embed.add_field(name="Content", value=message.content[:1024] or "*No content*", inline=False)
    await log_embed(message.guild, "message_log_channel", embed)

@bot.event
async def on_message_edit(before, after):
    if not before.guild or before.author.bot or before.content == after.content:
        return
    embed = discord.Embed(title="📝 Message Edited", color=discord.Color.blue(), timestamp=after.edited_at or discord.utils.utcnow())
    embed.add_field(name="Author", value=f"{before.author.mention} ({before.author.id})", inline=True)
    embed.add_field(name="Channel", value=before.channel.mention, inline=True)
    embed.add_field(name="Message ID", value=str(before.id), inline=True)
    embed.add_field(name="Before", value=before.content[:1024] or "*No content*", inline=False)
    embed.add_field(name="After", value=after.content[:1024] or "*No content*", inline=False)
    await log_embed(before.guild, "message_log_channel", embed)

@bot.event
async def on_member_join(member):
    # Anti-raid join rate detection and quarantine
    now_sec = _now_sec()
    win = JOIN_WINDOW.get(member.guild.id, [])
    win = [t for t in win if now_sec - t < 60]
    win.append(now_sec)
    JOIN_WINDOW[member.guild.id] = win
    if len(win) >= 10:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR IGNORE INTO guild_config (guild_id) VALUES (?)', (member.guild.id,))
            await db.execute('UPDATE guild_config SET raid_mode = 1 WHERE guild_id = ?', (member.guild.id,))
            await db.commit()
    acc_age_days = (discord.utils.utcnow() - member.created_at).days
    if acc_age_days < 3:
        role = await _ensure_quarantine_role(member.guild)
        if role:
            try:
                await member.add_roles(role, reason="Account too new (anti-raid)")
            except:
                pass

    # Welcome system (embed-only)
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT welcome_channel, welcome_embed_json FROM welcome_farewell WHERE guild_id = ?', (member.guild.id,)) as cursor:
            row = await cursor.fetchone()
    
    if row and row['welcome_channel']:
        channel = member.guild.get_channel(int(row['welcome_channel']))
        if not channel:
            try: channel = await member.guild.fetch_channel(int(row['welcome_channel']))
            except: pass
            
        if channel:
            embed_json = row['welcome_embed_json']
            placeholders = {
                "{user}": member.mention,
                "{username}": member.name,
                "{server}": member.guild.name,
                "{member_count}": str(member.guild.member_count),
                "{avatar}": member.display_avatar.url,
                "{join_date}": member.joined_at.strftime("%b %d, %Y")
            }
            embed = None
            if embed_json:
                try:
                    data = json.loads(embed_json)
                    def replace_in_dict(d):
                        if isinstance(d, str):
                            for key, val in placeholders.items():
                                d = d.replace(key, val)
                            return d
                        if isinstance(d, dict):
                            return {k: replace_in_dict(v) for k, v in d.items()}
                        if isinstance(d, list):
                            return [replace_in_dict(i) for i in d]
                        return d
                    data = replace_in_dict(data)
                    embed = discord.Embed.from_dict(data)
                except:
                    pass
            if embed is None:
                embed = discord.Embed(
                    title=f"👋 Welcome {member.name}",
                    description=f"Glad to have you in {member.guild.name}! You are member #{member.guild.member_count}.",
                    color=0x00d2ff,
                    timestamp=discord.utils.utcnow()
                )
            if member.display_avatar:
                try:
                    embed.set_thumbnail(url=member.display_avatar.url)
                except:
                    pass
            class WelcomeView(discord.ui.View):
                def __init__(self):
                    super().__init__(timeout=None)
                @discord.ui.button(label="Say Hi 👋", style=discord.ButtonStyle.primary, custom_id="welcome_hi")
                async def hi(self, interaction: discord.Interaction, button: discord.ui.Button):
                    await interaction.response.send_message(f"Welcome {member.mention}!", ephemeral=True)
            try:
                await channel.send(embed=embed, view=WelcomeView())
            except:
                pass

@bot.event
async def on_member_remove(member):
    # Log the event
    embed_log = discord.Embed(title="📤 Member Left", color=discord.Color.red(), timestamp=discord.utils.utcnow())
    embed_log.add_field(name="User", value=f"{member.mention} ({member.id})", inline=True)
    embed_log.set_thumbnail(url=member.display_avatar.url)
    await log_embed(member.guild, "leave_log_channel", embed_log)

    # Farewell system
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT farewell_channel, farewell_message FROM welcome_farewell WHERE guild_id = ?', (member.guild.id,)) as cursor:
            row = await cursor.fetchone()
    
    if row and row['farewell_channel']:
        channel = member.guild.get_channel(int(row['farewell_channel']))
        if not channel:
            try: channel = await member.guild.fetch_channel(int(row['farewell_channel']))
            except: pass
            
        if channel:
            msg = row['farewell_message'] or "Goodbye {user}!"
            msg = msg.replace("{user}", member.name).replace("{guild}", member.guild.name)
            try:
                await channel.send(msg)
            except:
                pass
    embed_log.add_field(name="Account Created", value=member.created_at.strftime("%b %d, %Y"), inline=True)
    embed_log.set_thumbnail(url=member.display_avatar.url)
    await log_embed(member.guild, "leave_log_channel", embed_log)

    # Welcome system
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM welcome_farewell WHERE guild_id = ?', (member.guild.id,)) as cursor:
            config = await cursor.fetchone()
    
    if not config or not config['welcome_channel']:
        return
        
    channel = member.guild.get_channel(config['welcome_channel'])
    if not channel:
        try: channel = await member.guild.fetch_channel(config['welcome_channel'])
        except: return

    message = config['welcome_message'] or "Welcome {user} to {server}!"
    embed_json = config['welcome_embed_json']
    
    # Replace placeholders
    placeholders = {
        "{user}": member.mention,
        "{username}": member.name,
        "{server}": member.guild.name,
        "{member_count}": str(member.guild.member_count),
        "{avatar}": member.display_avatar.url,
        "{join_date}": member.joined_at.strftime("%b %d, %Y")
    }
    
    final_message = message
    for key, val in placeholders.items():
        final_message = final_message.replace(key, val)

    embed = None
    if embed_json:
        try:
            data = json.loads(embed_json)
            # Placeholder replacement in embed data
            def replace_in_dict(d):
                if isinstance(d, str):
                    for key, val in placeholders.items():
                        d = d.replace(key, val)
                    return d
                if isinstance(d, dict):
                    return {k: replace_in_dict(v) for k, v in d.items()}
                if isinstance(d, list):
                    return [replace_in_dict(i) for i in d]
                return d
            
            data = replace_in_dict(data)
            embed = discord.Embed.from_dict(data)
        except:
            pass

    # Create Button View if needed (example: a button that shows server info or a welcome message)
    class WelcomeView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=None)
            
        @discord.ui.button(label="Server Info", style=discord.ButtonStyle.primary, custom_id="welcome_server_info")
        async def server_info(self, interaction: discord.Interaction, button: discord.ui.Button):
            guild = interaction.guild
            embed = discord.Embed(title=f"🏰 {guild.name} Info", color=0x00d2ff)
            embed.add_field(name="Members", value=str(guild.member_count))
            embed.add_field(name="Owner", value=guild.owner.mention)
            await interaction.response.send_message(embed=embed, ephemeral=True)

    await channel.send(content=final_message, embed=embed, view=WelcomeView())

@bot.event
async def on_member_remove(member):
    # Log the event
    embed_log = discord.Embed(title="📤 Member Left", color=discord.Color.red(), timestamp=discord.utils.utcnow())
    embed_log.add_field(name="User", value=f"{member} ({member.id})", inline=True)
    embed_log.set_thumbnail(url=member.display_avatar.url)
    await log_embed(member.guild, "leave_log_channel", embed_log)

    # Farewell system
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM welcome_farewell WHERE guild_id = ?', (member.guild.id,)) as cursor:
            config = await cursor.fetchone()
    
    if not config or not config['farewell_channel']:
        return
        
    channel = member.guild.get_channel(config['farewell_channel'])
    if not channel:
        try: channel = await member.guild.fetch_channel(config['farewell_channel'])
        except: return

    message = config['farewell_message'] or "{user} has left the server."
    farewell_message = message.replace("{user}", str(member)).replace("{server}", member.guild.name)
    
    await channel.send(farewell_message)

@bot.event
async def on_guild_channel_create(channel):
    embed = discord.Embed(title="📁 Channel Created", color=discord.Color.green(), timestamp=discord.utils.utcnow())
    embed.add_field(name="Name", value=channel.name, inline=True)
    embed.add_field(name="Type", value=str(channel.type), inline=True)
    embed.add_field(name="Category", value=channel.category.name if channel.category else "None", inline=True)
    await log_embed(channel.guild, "server_log_channel", embed)

@bot.event
async def on_guild_channel_delete(channel):
    embed = discord.Embed(title="📁 Channel Deleted", color=discord.Color.red(), timestamp=discord.utils.utcnow())
    embed.add_field(name="Name", value=channel.name, inline=True)
    embed.add_field(name="Type", value=str(channel.type), inline=True)
    await log_embed(channel.guild, "server_log_channel", embed)

async def log_mod_action(guild, action, target, moderator, reason, duration=None):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT mod_log_channel FROM logging_config WHERE guild_id = ?', (guild.id,)) as cursor:
            row = await cursor.fetchone()
            if not row or not row[0]:
                return
            channel_id = row[0]
            channel = guild.get_channel(channel_id)
            if not channel:
                # Try to fetch if not in cache
                try:
                    channel = await guild.fetch_channel(channel_id)
                except:
                    return

            embed = discord.Embed(title=f"Moderation Action: {action}", color=discord.Color.red())
            embed.add_field(name="Target", value=f"{target} ({target.id})", inline=False)
            embed.add_field(name="Moderator", value=f"{moderator} ({moderator.id})", inline=False)
            embed.add_field(name="Reason", value=reason, inline=False)
            if duration:
                embed.add_field(name="Duration", value=duration, inline=False)
            embed.timestamp = discord.utils.utcnow()
            
            # Retry mechanism
            for attempt in range(3):
                try:
                    await channel.send(embed=embed)
                    break
                except discord.HTTPException as e:
                    if attempt == 2:
                        print(f"Failed to send mod log to {channel_id} after 3 attempts: {e}")
                    await asyncio.sleep(1 * (attempt + 1))
                except Exception as e:
                    print(f"Error logging mod action: {e}")
                    break

async def log_embed(guild, column, embed):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(f'SELECT {column}, use_webhooks FROM logging_config WHERE guild_id = ?', (guild.id,)) as cursor:
            row = await cursor.fetchone()
            if not row or not row[0]:
                return
            try:
                channel_id = int(row[0])
            except Exception:
                channel_id = row[0]

            channel = guild.get_channel(channel_id)
            if not channel:
                try:
                    channel = await guild.fetch_channel(channel_id)
                except:
                    return

            use_webhooks = 0
            try:
                use_webhooks = int(row[1] or 0)
            except:
                use_webhooks = 0

            if channel and use_webhooks == 1:
                try:
                    whs = await channel.webhooks()
                    wh = whs[0] if whs else None
                    if not wh:
                        wh = await channel.create_webhook(name="EmpireNexus Logs")
                    await wh.send(embed=embed, username=guild.me.display_name if guild.me else "EmpireNexus", avatar_url=guild.me.display_avatar.url if guild.me else None)
                    return
                except:
                    pass

            for attempt in range(3):
                try:
                    await channel.send(embed=embed)
                    break
                except discord.HTTPException as e:
                    if attempt == 2:
                        print(f"Failed to send log to {channel_id} ({column}) after 3 attempts: {e}")
                    await asyncio.sleep(1 * (attempt + 1))
                except Exception as e:
                    print(f"Error logging embed ({column}): {e}")
                    break

def parse_duration(duration_str):
    if not duration_str:
        return None
    
    total_seconds = 0
    import re
    matches = re.findall(r'(\d+)([smhd])', duration_str.lower())
    if not matches:
        return None
    
    for amount, unit in matches:
        amount = int(amount)
        if unit == 's': total_seconds += amount
        elif unit == 'm': total_seconds += amount * 60
        elif unit == 'h': total_seconds += amount * 3600
        elif unit == 'd': total_seconds += amount * 86400
    
    return total_seconds

def can_act_on(actor: discord.Member, target: discord.Member) -> bool:
    if actor.id in BOT_OWNERS or actor.guild.owner_id == actor.id:
        return True
    if actor.id == target.id:
        return False
    try:
        return actor.top_role > target.top_role
    except:
        return False

# --- MODERATION COMMANDS ---

@bot.hybrid_command(name="kick", description="Remove a member from the server")
@owner_or_has(kick_members=True)
@app_commands.describe(member="The member to kick", reason="Reason for kicking", duration="Optional time (e.g. 1h, 1d) - will be logged")
async def kick(ctx: commands.Context, member: discord.Member, reason: str = "No reason provided", duration: str = None):
    if not can_act_on(ctx.author, member):
        return await ctx.send("❌ You cannot kick someone with a higher or equal role!")
    
    try:
        await member.kick(reason=reason)
        await ctx.send(f"✅ **{member.display_name}** has been kicked. Reason: {reason}")
        await log_mod_action(ctx.guild, "Kick", member, ctx.author, reason, duration)
    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to kick this member.")

@bot.hybrid_command(name="ban", description="Ban a member from the server")
@owner_or_has(ban_members=True)
@app_commands.describe(member="The member to ban", reason="Reason for banning", duration="Duration (e.g. 1h, 1d)")
async def ban(ctx: commands.Context, member: discord.Member, reason: str = "No reason provided", duration: str = None):
    if not can_act_on(ctx.author, member):
        return await ctx.send("❌ You cannot ban someone with a higher or equal role!")

    seconds = parse_duration(duration)
    
    try:
        await member.ban(reason=reason)
        await ctx.send(f"✅ **{member.display_name}** has been banned. Reason: {reason}" + (f" for {duration}" if duration else ""))
        await log_mod_action(ctx.guild, "Ban", member, ctx.author, reason, duration)
        
        if seconds:
            # We would need a background task to unban, but for now we'll just log it.
            # In a real production bot, you'd store this in DB and have a loop.
            pass
    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to ban this member.")

@bot.hybrid_command(name="warn", description="Issue a warning to a member")
@owner_or_has(kick_members=True)
@app_commands.describe(member="The member to warn", reason="Reason for warning", duration="Expiration time (e.g. 1d, 30d)")
async def warn(ctx: commands.Context, member: discord.Member, reason: str = "No reason provided", duration: str = None):
    if not can_act_on(ctx.author, member):
        return await ctx.send("❌ You cannot warn someone with a higher or equal role!")

    seconds = parse_duration(duration)
    expires_at = int(time.time() + seconds) if seconds else None
    
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            INSERT INTO warnings (user_id, guild_id, moderator_id, reason, timestamp, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (member.id, ctx.guild.id, ctx.author.id, reason, int(time.time()), expires_at))
        await db.commit()
    
    await ctx.send(f"⚠️ **{member.display_name}** has been warned. Reason: {reason}")
    await log_mod_action(ctx.guild, "Warning", member, ctx.author, reason, duration)

@bot.hybrid_command(name="clearwarnings", description="Clears all warnings of a user")
@owner_or_has(kick_members=True)
@app_commands.describe(user="The user to clear warnings for")
async def clearwarnings_standalone(ctx: commands.Context, user: discord.User):
    target_member = ctx.guild.get_member(user.id)
    if target_member and not can_act_on(ctx.author, target_member):
        return await ctx.send("❌ You cannot modify warnings for someone with a higher or equal role!")
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('DELETE FROM warnings WHERE user_id = ? AND guild_id = ?', (user.id, ctx.guild.id))
        await db.commit()
    
    await ctx.send(f"✅ Cleared all warnings for **{user.display_name}**.")
    await log_mod_action(ctx.guild, "Clear Warnings", user, ctx.author, "All warnings cleared")

@bot.hybrid_command(name="delwarn", description="Delete a specific warning by ID")
@owner_or_has(kick_members=True)
@app_commands.describe(id="The ID of the warning to remove")
async def delwarn_standalone(ctx: commands.Context, id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT user_id FROM warnings WHERE warn_id = ? AND guild_id = ?', (id, ctx.guild.id)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return await ctx.send(f"❌ Warning ID `{id}` not found in this server.")
            
            user_id = row[0]
            target_member = ctx.guild.get_member(user_id)
            if target_member and not can_act_on(ctx.author, target_member):
                return await ctx.send("❌ You cannot modify warnings for someone with a higher or equal role!")
            await db.execute('DELETE FROM warnings WHERE warn_id = ?', (id,))
            await db.commit()
    
    user = bot.get_user(user_id) or f"User ({user_id})"
    await ctx.send(f"✅ Removed warning `{id}` from **{user}**.")
    await log_mod_action(ctx.guild, "Remove Warning", user, ctx.author, f"Warning ID {id} removed")

@bot.hybrid_group(name="warnings", description="Display warning history for a user")
async def warnings_group(ctx: commands.Context, user: discord.User):
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM warnings WHERE user_id = ? AND guild_id = ? ORDER BY timestamp DESC', (user.id, ctx.guild.id)) as cursor:
            rows = await cursor.fetchall()
    
    if not rows:
        return await ctx.send(f"✅ {user.display_name} has no warnings.")
    
    embed = discord.Embed(title=f"Warnings for {user.display_name}", color=discord.Color.orange())
    for row in rows:
        moderator = ctx.guild.get_member(row['moderator_id']) or f"Unknown ({row['moderator_id']})"
        expiry = f"\nExpires: <t:{row['expires_at']}:R>" if row['expires_at'] else ""
        embed.add_field(
            name=f"ID: {row['warn_id']} | <t:{row['timestamp']}:R>",
            value=f"**Reason:** {row['reason']}\n**Moderator:** {moderator}{expiry}",
            inline=False
        )
    await ctx.send(embed=apply_theme(embed))

@warnings_group.command(name="clear", description="Purge all warnings for a specified user")
@owner_or_has(kick_members=True)
async def clear_warnings(ctx: commands.Context, user: discord.User):
    target_member = ctx.guild.get_member(user.id)
    if target_member and not can_act_on(ctx.author, target_member):
        return await ctx.send("❌ You cannot modify warnings for someone with a higher or equal role!")
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('DELETE FROM warnings WHERE user_id = ? AND guild_id = ?', (user.id, ctx.guild.id))
        await db.commit()
    
    await ctx.send(f"✅ Cleared all warnings for **{user.display_name}**.")
    await log_mod_action(ctx.guild, "Clear Warnings", user, ctx.author, "All warnings cleared")

# --- Utility Moderation Commands ---

@bot.hybrid_command(name="purge", description="Delete a number of messages from this channel")
@owner_or_has(manage_messages=True)
@app_commands.describe(count="Number of messages to delete (1-100)")
async def purge(ctx: commands.Context, count: int):
    if count < 1 or count > 100:
        return await ctx.send("❌ Please provide a count between 1 and 100.")
    try:
        deleted = await ctx.channel.purge(limit=count, bulk=True)
        await ctx.send(f"🧹 Deleted {len(deleted)} messages.", delete_after=5)
        embed = discord.Embed(title="Bulk Message Delete", color=discord.Color.dark_red(), timestamp=discord.utils.utcnow())
        embed.add_field(name="Channel", value=ctx.channel.mention, inline=False)
        embed.add_field(name="Count", value=str(len(deleted)), inline=True)
        embed.add_field(name="Actor", value=f"{ctx.author} ({ctx.author.id})", inline=False)
        await log_embed(ctx.guild, "message_log_channel", embed)
        await log_mod_action(ctx.guild, "Purge", ctx.channel, ctx.author, f"Deleted {len(deleted)} messages")
    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to manage messages here.")
    except Exception as e:
        await ctx.send(f"❌ Error while purging: {e}")

@bot.hybrid_command(name="setnick", description="Set a member's nickname")
@owner_or_has(manage_nicknames=True)
@app_commands.describe(member="Member to rename", nickname="New nickname")
async def setnick(ctx: commands.Context, member: discord.Member, *, nickname: str):
    if len(nickname) > 32:
        return await ctx.send("❌ Nickname must be 32 characters or fewer.")
    if not can_act_on(ctx.author, member):
        return await ctx.send("❌ You cannot change the nickname of someone with a higher or equal role!")
    try:
        await member.edit(nick=nickname, reason=f"Set by {ctx.author}")
        await ctx.send(f"✅ Changed nickname for **{member.display_name}** to **{nickname}**.")
        await log_mod_action(ctx.guild, "Set Nickname", member, ctx.author, f"Nickname → {nickname}")
    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to change that member's nickname.")
    except Exception as e:
        await ctx.send(f"❌ Error changing nickname: {e}")

@bot.hybrid_command(name="timeout", description="Timeout a member for a duration")
@owner_or_has(moderate_members=True)
@app_commands.describe(member="Member to timeout", duration="e.g. 30m, 2h, 1d", reason="Reason")
async def timeout(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str = "No reason provided"):
    if not can_act_on(ctx.author, member):
        return await ctx.send("❌ You cannot timeout someone with a higher or equal role!")
    seconds = parse_duration(duration)
    if not seconds or seconds < 60:
        return await ctx.send("❌ Duration must be at least 1 minute. Use formats like 30m, 2h, 1d.")
    try:
        from datetime import timedelta, datetime
        try:
            await member.timeout(timedelta(seconds=seconds), reason=reason)
        except:
            until = datetime.utcnow() + timedelta(seconds=seconds)
            await member.edit(communication_disabled_until=until, reason=reason)
        await ctx.send(f"⏳ Timed out **{member.display_name}** for **{duration}**. Reason: {reason}")
        await log_mod_action(ctx.guild, "Timeout", member, ctx.author, reason, duration)
    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to timeout that member.")
    except Exception as e:
        await ctx.send(f"❌ Error applying timeout: {e}")

@bot.hybrid_command(name="removewarn", description="Delete a specific warning by ID")
@owner_or_has(kick_members=True)
@app_commands.describe(warn_id="The ID of the warning to remove")
async def remove_warn(ctx: commands.Context, warn_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        # Check if warning exists and belongs to this guild
        async with db.execute('SELECT user_id FROM warnings WHERE warn_id = ? AND guild_id = ?', (warn_id, ctx.guild.id)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return await ctx.send(f"❌ Warning ID `{warn_id}` not found in this server.")
            
            user_id = row[0]
            await db.execute('DELETE FROM warnings WHERE warn_id = ?', (warn_id,))
            await db.commit()
    
    user = bot.get_user(user_id) or f"User ({user_id})"
    await ctx.send(f"✅ Removed warning `{warn_id}` from **{user}**.")
    await log_mod_action(ctx.guild, "Remove Warning", user, ctx.author, f"Warning ID {warn_id} removed")

# --- DASHBOARD CONFIGURABLE FEATURES ---

@bot.hybrid_group(name="set", description="Configure server settings")
@owner_or_has(manage_guild=True)
async def set_group(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        await ctx.send("❌ Use `/set welcome` or `/set farewell`.")

@set_group.command(name="welcome", description="Configure welcome messages")
@app_commands.describe(channel="Channel for welcome messages", message="Welcome message text (use {user} for mention)", embed_json="JSON for embed (optional)")
async def set_welcome(ctx: commands.Context, channel: str, message: str, embed_json: str = None):
    # Try to convert channel to int if it's an ID string from autocomplete
    try:
        if channel.isdigit():
            channel_id = int(channel)
        else:
            channel_id = int(channel.replace("<#", "").replace(">", ""))
        discord_channel = ctx.guild.get_channel(channel_id)
    except:
        return await ctx.send("❌ Invalid channel! Please select a channel from the autocomplete list or mention it.")

    if not discord_channel:
        return await ctx.send("❌ Channel not found!")

    if embed_json:
        try:
            json.loads(embed_json)
        except:
            return await ctx.send("❌ Invalid JSON for embed! Please provide a valid JSON string.")

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            INSERT INTO welcome_farewell (guild_id, welcome_channel, welcome_message, welcome_embed_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET 
                welcome_channel=excluded.welcome_channel, 
                welcome_message=excluded.welcome_message,
                welcome_embed_json=excluded.welcome_embed_json
        ''', (ctx.guild.id, discord_channel.id, message, embed_json))
        await db.commit()
    
    await ctx.send(f"✅ Welcome messages set to {discord_channel.mention}.\n**Message:** {message}" + ("\n**Embed:** Enabled" if embed_json else ""))

@set_welcome.autocomplete("channel")
async def welcome_channel_autocomplete(interaction: discord.Interaction, current: str):
    channels = [c for c in interaction.guild.text_channels if current.lower() in c.name.lower()]
    return [app_commands.Choice(name=c.name, value=str(c.id)) for c in channels[:25]]

@set_group.command(name="welcome_preview", description="Preview your current welcome message configuration")
async def welcome_preview(ctx: commands.Context):
    # ... existing implementation ...
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM welcome_farewell WHERE guild_id = ?', (ctx.guild.id,)) as cursor:
            config = await cursor.fetchone()
    
    if not config or not config['welcome_channel']:
        return await ctx.send("❌ Welcome messages are not configured!")

    member = ctx.author
    message = config['welcome_message'] or "Welcome {user} to {server}!"
    embed_json = config['welcome_embed_json']
    
    placeholders = {
        "{user}": member.mention,
        "{username}": member.name,
        "{server}": ctx.guild.name,
        "{member_count}": str(ctx.guild.member_count),
        "{avatar}": member.display_avatar.url,
        "{join_date}": member.joined_at.strftime("%b %d, %Y")
    }
    
    final_message = message
    for key, val in placeholders.items():
        final_message = final_message.replace(key, val)

    embed = None
    if embed_json:
        try:
            data = json.loads(embed_json)
            def replace_in_dict(d):
                if isinstance(d, str):
                    for key, val in placeholders.items():
                        d = d.replace(key, val)
                    return d
                if isinstance(d, dict):
                    return {k: replace_in_dict(v) for k, v in d.items()}
                if isinstance(d, list):
                    return [replace_in_dict(i) for i in d]
                return d
            data = replace_in_dict(data)
            embed = discord.Embed.from_dict(data)
        except Exception as e:
            await ctx.send(f"⚠️ Error parsing embed JSON: {e}")

    await ctx.send("👀 **Welcome Preview:**", content=final_message, embed=embed)

@set_group.command(name="farewell", description="Configure farewell messages")
@app_commands.describe(channel="Channel for farewell messages", message="Farewell message text (use {user} for name)")
async def set_farewell(ctx: commands.Context, channel: str, *, message: str):
    # Try to convert channel to int if it's an ID string from autocomplete
    try:
        if channel.isdigit():
            channel_id = int(channel)
        else:
            channel_id = int(channel.replace("<#", "").replace(">", ""))
        discord_channel = ctx.guild.get_channel(channel_id)
    except:
        return await ctx.send("❌ Invalid channel! Please select a channel from the autocomplete list or mention it.")

    if not discord_channel:
        return await ctx.send("❌ Channel not found!")

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            INSERT INTO welcome_farewell (guild_id, farewell_channel, farewell_message)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET farewell_channel=excluded.farewell_channel, farewell_message=excluded.farewell_message
        ''', (ctx.guild.id, discord_channel.id, message))
        await db.commit()
    
    await ctx.send(f"✅ Farewell messages set to {discord_channel.mention}.\n**Message:** {message}")

@set_farewell.autocomplete("channel")
async def farewell_channel_autocomplete(interaction: discord.Interaction, current: str):
    channels = [c for c in interaction.guild.text_channels if current.lower() in c.name.lower()]
    return [app_commands.Choice(name=c.name, value=str(c.id)) for c in channels[:25]]

@bot.hybrid_command(name="setlogs", description="Configure logging channels")
@owner_or_admin()
@app_commands.describe(category="Log category", channel="Channel to send logs to")
@app_commands.choices(category=[
    app_commands.Choice(name="Message Logs", value="message_log_channel"),
    app_commands.Choice(name="Member Logs", value="member_log_channel"),
    app_commands.Choice(name="Join Logs", value="join_log_channel"),
    app_commands.Choice(name="Leave Logs", value="leave_log_channel"),
    app_commands.Choice(name="Server Logs", value="server_log_channel"),
    app_commands.Choice(name="Mod Logs", value="mod_log_channel"),
    app_commands.Choice(name="Automod Logs", value="automod_log_channel")
])
async def set_logs(ctx: commands.Context, category: str, channel: str):
    # Try to convert channel to int if it's an ID string from autocomplete
    try:
        if channel.isdigit():
            channel_id = int(channel)
        else:
            channel_id = int(channel.replace("<#", "").replace(">", ""))
        discord_channel = ctx.guild.get_channel(channel_id)
    except:
        return await ctx.send("❌ Invalid channel! Please select a channel from the autocomplete list or mention it.")

    if not discord_channel:
        return await ctx.send("❌ Channel not found!")

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(f'''
            INSERT INTO logging_config (guild_id, {category}) VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET {category} = excluded.{category}
        ''', (ctx.guild.id, discord_channel.id))
        await db.commit()
    await ctx.send(f"✅ Logging for **{category.replace('_', ' ').title()}** set to {discord_channel.mention}")

@set_logs.autocomplete("channel")
async def logs_channel_autocomplete(interaction: discord.Interaction, current: str):
    channels = [c for c in interaction.guild.text_channels if current.lower() in c.name.lower()]
    return [app_commands.Choice(name=c.name, value=str(c.id)) for c in channels[:25]]

@bot.hybrid_group(name="automod", description="Manage automatic moderation")
@owner_or_has(manage_guild=True)
async def automod_group(ctx: commands.Context):
    if ctx.invoked_subcommand is None:
        await ctx.send("❌ Use `/automod add` or `/automod remove`.")

@automod_group.command(name="add", description="Add a word to the filter")
@app_commands.describe(word="The word to filter", punishment="Punishment (warn/kick/ban/delete)")
async def automod_add(ctx: commands.Context, word: str, punishment: str = "warn"):
    punishment = punishment.lower()
    if punishment not in ['warn', 'kick', 'ban', 'delete']:
        return await ctx.send("❌ Invalid punishment! Choose: `warn`, `kick`, `ban`, or `delete`.")
    
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT INTO automod_words (guild_id, word, punishment) VALUES (?, ?, ?)', 
                        (ctx.guild.id, word.lower(), punishment))
        await db.commit()
    
    await ctx.send(f"✅ Added `{word}` to the word filter with punishment: **{punishment}**.")

@automod_group.command(name="remove", description="Remove a word from the filter by ID")
@app_commands.describe(word_id="The ID of the word to remove")
async def automod_remove(ctx: commands.Context, word_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT word FROM automod_words WHERE word_id = ? AND guild_id = ?', (word_id, ctx.guild.id)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return await ctx.send(f"❌ Word ID `{word_id}` not found.")
            
            word = row[0]
            await db.execute('DELETE FROM automod_words WHERE word_id = ?', (word_id,))
            await db.commit()
    
    await ctx.send(f"✅ Removed `{word}` from the word filter.")

@bot.hybrid_command(name="reactionroles", description="Create a reaction role message")
@owner_or_has(manage_roles=True)
@app_commands.describe(message_id="The ID of the message to add reaction roles to", emoji="The emoji to use", role="The role to assign")
async def reaction_roles(ctx: commands.Context, message_id: str, emoji: str, role: discord.Role):
    try:
        msg_id = int(message_id)
        msg = await ctx.channel.fetch_message(msg_id)
    except:
        return await ctx.send("❌ Invalid message ID or message not found in this channel.")

    try:
        await msg.add_reaction(emoji)
    except:
        return await ctx.send("❌ I couldn't add that reaction. Make sure I have permission and it's a valid emoji.")

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR REPLACE INTO reaction_roles (message_id, guild_id, emoji, role_id) VALUES (?, ?, ?, ?)',
                        (msg_id, ctx.guild.id, emoji, role.id))
        await db.commit()
    
    await ctx.send(f"✅ Reaction role added! Users reacting with {emoji} to [that message]({msg.jump_url}) will get the **{role.name}** role.")

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None or payload.user_id is None:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    emoji_key = str(payload.emoji)
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT role_id FROM reaction_roles WHERE message_id = ? AND guild_id = ? AND emoji = ?', (payload.message_id, payload.guild_id, emoji_key)) as cursor:
            row = await cursor.fetchone()
    if not row:
        return
    role = guild.get_role(row[0])
    if not role:
        return
    member = guild.get_member(payload.user_id)
    if not member or member.bot:
        return
    try:
        await member.add_roles(role, reason="Reaction Roles")
    except:
        pass

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None or payload.user_id is None:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    emoji_key = str(payload.emoji)
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT role_id FROM reaction_roles WHERE message_id = ? AND guild_id = ? AND emoji = ?', (payload.message_id, payload.guild_id, emoji_key)) as cursor:
            row = await cursor.fetchone()
    if not row:
        return
    role = guild.get_role(row[0])
    if not role:
        return
    member = guild.get_member(payload.user_id)
    if not member or member.bot:
        return
    try:
        await member.remove_roles(role, reason="Reaction Roles")
    except:
        pass

async def run_custom_command(code: str, message: discord.Message):
    # Allow some common imports if they are at the very top, but better to provide them in globals
    # We keep the check but relax it for known safe patterns if needed, 
    # but for now let's just provide the libraries.
    if "__" in code and "__cmd__" not in code: # Allow our internal func name
        return False, "Disallowed code (contains __)."
    
    func_name = "__cmd__"
    src = "async def " + func_name + "(message, bot):\n"
    for line in code.splitlines():
        # Strip potential imports from user code to avoid the 'import' check failure
        if line.strip().startswith(("import discord", "import json", "import asyncio", "from discord")):
            continue
        src += "    " + line + "\n"
    
    sandbox_globals = {
        "__builtins__": {
            "len": len, "str": str, "int": int, "float": float, 
            "min": min, "max": max, "range": range, "list": list, "dict": dict,
            "sum": sum, "any": any, "all": all, "print": print
        },
        "discord": discord,
        "json": json,
        "asyncio": asyncio,
        "time": time,
        "random": random
    }
    sandbox_locals = {}
    try:
        exec(src, sandbox_globals, sandbox_locals)
        fn = sandbox_locals.get(func_name)
        if not fn:
            return False, "Code error: function not defined."
        await asyncio.wait_for(fn(message, bot), timeout=5.0)
        return True, None
    except Exception as e:
        return False, str(e)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    content = message.content.lower()
    prefix = await get_prefix(bot, message)
    is_command = message.content.startswith(prefix)
    
    # --- AUTOMOD & CUSTOM COMMANDS (Optimized DB usage) ---
    async with aiosqlite.connect(DB_FILE) as db:
        # Check Automod
        async with db.execute('SELECT word, punishment FROM automod_words WHERE guild_id = ?', (message.guild.id,)) as cursor:
            rows = await cursor.fetchall()
            for word, punishment in rows:
                if word in content:
                    if punishment == "delete":
                        try: await message.delete()
                        except: pass
                    elif punishment == "warn":
                        await db.execute('INSERT INTO warnings (user_id, guild_id, moderator_id, reason, timestamp) VALUES (?, ?, ?, ?, ?)', 
                                         (message.author.id, message.guild.id, bot.user.id, f"AutoMod: {word}", int(time.time())))
                        await db.commit()
                        try: await message.delete()
                        except: pass
                    elif punishment == "kick":
                        try: await message.author.kick(reason=f"AutoMod: {word}")
                        except: pass
                    elif punishment == "ban":
                        try: await message.author.ban(reason=f"AutoMod: {word}")
                        except: pass
                    
                    embed = discord.Embed(title="AutoMod Triggered", color=discord.Color.red())
                    embed.add_field(name="User", value=f"{message.author} ({message.author.id})", inline=False)
                    embed.add_field(name="Word", value=word, inline=False)
                    embed.add_field(name="Channel", value=f"{message.channel.mention}", inline=False)
                    embed.add_field(name="Content", value=message.content[:512], inline=False)
                    await log_embed(message.guild, "automod_log_channel", embed)
                    return # Stop processing if punished

        # Check Custom Commands if it starts with prefix
        if is_command:
            cmd_name = message.content[len(prefix):].split()[0]
            async with db.execute('SELECT code FROM custom_commands WHERE guild_id = ? AND name = ?', (message.guild.id, cmd_name)) as cursor:
                row = await cursor.fetchone()
                if row:
                    code = row[0]
                    ok, err = await run_custom_command(code, message)
                    embed = discord.Embed(title="Custom Command Executed", color=discord.Color.blurple())
                    embed.add_field(name="User", value=f"{message.author} ({message.author.id})", inline=False)
                    embed.add_field(name="Command", value=cmd_name, inline=False)
                    if err:
                        embed.add_field(name="Error", value=str(err)[:300], inline=False)
                    await log_embed(message.guild, "command_log_channel", embed)
                    return # Custom command handled, don't process regular commands

    # Anti-phishing simple pattern
    try:
        cfg = await _cfg_get(message.guild.id, ["raid_mode","anti_phish_enabled","mod_message_point"])
        if int(cfg.get("anti_phish_enabled", 1) or 1) == 1:
            content_low = message.content.lower()
            if ("free nitro" in content_low or "discordgift" in content_low or "airdrop" in content_low) and ("http" in content_low or "www" in content_low):
                try:
                    await message.delete()
                except:
                    pass
                try:
                    await message.author.timeout(discord.utils.timedelta(minutes=10), reason="Phishing attempt")
                except:
                    pass
        # Flood detection per-user
        now = _now_sec()
        ukey = (message.guild.id, message.author.id)
        arr = MESSAGE_WINDOW.get(ukey, [])
        arr = [t for t in arr if now - t < 10]
        arr.append(now)
        MESSAGE_WINDOW[ukey] = arr
        if len(arr) >= 8:
            try:
                await message.author.timeout(discord.utils.timedelta(minutes=15), reason="Message flood")
            except:
                pass
        # Mod points for staff messages
        if message.author.guild_permissions.manage_messages:
            pts = int(cfg.get("mod_message_point", 1) or 1)
            await add_mod_points(message.author.id, message.guild.id, pts)
    except:
        pass
    await bot.process_commands(message)

@bot.event
async def on_raw_bulk_message_delete(payload: discord.RawBulkMessageDeleteEvent):
    if not payload.guild_id:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    embed = discord.Embed(title="Bulk Message Delete", color=discord.Color.dark_red())
    embed.add_field(name="Channel", value=f"<#{payload.channel_id}>", inline=False)
    embed.add_field(name="Count", value=str(len(payload.message_ids)), inline=False)
    await log_embed(guild, "message_log_channel", embed)

@bot.event
async def on_member_join(member: discord.Member):
    # This handler is now consolidated in the first on_member_join above.
    pass

@bot.event
async def on_member_remove(member: discord.Member):
    # This handler is now consolidated in the first on_member_remove above.
    pass

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if not after.guild:
        return
    changes = []
    if before.nick != after.nick:
        changes.append(("Nickname", f"{before.nick} → {after.nick}"))
    try:
        if str(before.display_avatar.url) != str(after.display_avatar.url):
            changes.append(("Avatar", "Changed"))
    except:
        pass
    before_roles = set(r.id for r in before.roles)
    after_roles = set(r.id for r in after.roles)
    added = after_roles - before_roles
    removed = before_roles - after_roles
    if added:
        names = [after.guild.get_role(r).name for r in added if after.guild.get_role(r)]
        changes.append(("Roles Added", ", ".join(names)))
    if removed:
        names = [after.guild.get_role(r).name for r in removed if after.guild.get_role(r)]
        changes.append(("Roles Removed", ", ".join(names)))
    if changes:
        embed = discord.Embed(title="Member Updated", color=discord.Color.blurple())
        embed.add_field(name="User", value=f"{after.mention} ({after.id})", inline=False)
        for k, v in changes:
            embed.add_field(name=k, value=v or "None", inline=False)
        try:
            embed.set_thumbnail(url=after.display_avatar.url)
        except:
            pass
        await log_embed(after.guild, "member_log_channel", embed)

@bot.event
async def on_user_update(before: discord.User, after: discord.User):
    embed = discord.Embed(title="User Updated", color=discord.Color.blurple())
    embed.add_field(name="User", value=f"<@{after.id}> ({after.id})", inline=False)
    if before.avatar != after.avatar:
        embed.add_field(name="Avatar", value="Changed", inline=False)
        try:
            embed.set_thumbnail(url=after.display_avatar.url)
        except:
            pass
    if before.global_name != after.global_name:
        embed.add_field(name="Global Name", value=f"{before.global_name} → {after.global_name}", inline=False)
    for guild in bot.guilds:
        if guild.get_member(after.id):
            await log_embed(guild, "member_log_channel", embed)

@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    actor, reason = await _find_actor(channel.guild, discord.AuditLogAction.channel_create, channel.id)
    embed = discord.Embed(title="Channel Created", color=discord.Color.green())
    embed.add_field(name="Channel", value=f"{channel.mention} ({channel.id})", inline=False)
    if actor:
        embed.add_field(name="Actor", value=f"{actor} ({actor.id})", inline=False)
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    await log_embed(channel.guild, "server_log_channel", embed)

@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    actor, reason = await _find_actor(channel.guild, discord.AuditLogAction.channel_delete, channel.id)
    embed = discord.Embed(title="Channel Deleted", color=discord.Color.dark_red())
    embed.add_field(name="Channel", value=f"#{channel.name} ({channel.id})", inline=False)
    if actor:
        embed.add_field(name="Actor", value=f"{actor} ({actor.id})", inline=False)
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    await log_embed(channel.guild, "server_log_channel", embed)

@bot.event
async def on_guild_channel_update(before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
    actor, reason = await _find_actor(after.guild, discord.AuditLogAction.channel_update, after.id)
    embed = discord.Embed(title="Channel Updated", color=discord.Color.orange())
    embed.add_field(name="Channel", value=f"{after.mention} ({after.id})", inline=False)
    if before.name != after.name:
        embed.add_field(name="Name", value=f"{before.name} → {after.name}", inline=False)
    topic_b = getattr(before, "topic", None)
    topic_a = getattr(after, "topic", None)
    if topic_b != topic_a:
        embed.add_field(name="Topic", value=f"{topic_b or 'None'} → {topic_a or 'None'}", inline=False)
    cat_b = before.category.name if before.category else "None"
    cat_a = after.category.name if after.category else "None"
    if cat_b != cat_a:
        embed.add_field(name="Category", value=f"{cat_b} → {cat_a}", inline=False)
    nsfw_b = getattr(before, "nsfw", None)
    nsfw_a = getattr(after, "nsfw", None)
    if nsfw_b != nsfw_a:
        embed.add_field(name="NSFW", value=f"{nsfw_b} → {nsfw_a}", inline=False)
    rate_b = getattr(before, "slowmode_delay", getattr(before, "rate_limit_per_user", None))
    rate_a = getattr(after, "slowmode_delay", getattr(after, "rate_limit_per_user", None))
    if rate_b != rate_a:
        embed.add_field(name="Slowmode", value=f"{rate_b} → {rate_a}", inline=False)
    pos_b = getattr(before, "position", None)
    pos_a = getattr(after, "position", None)
    if pos_b != pos_a:
        embed.add_field(name="Position", value=f"{pos_b} → {pos_a}", inline=False)
    try:
        ow_b = before.overwrites or {}
        ow_a = after.overwrites or {}
        if ow_b != ow_a:
            embed.add_field(name="Permissions", value="Changed", inline=False)
    except:
        pass
    if actor:
        embed.add_field(name="Actor", value=f"{actor} ({actor.id})", inline=False)
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    await log_embed(after.guild, "server_log_channel", embed)

@bot.event
async def on_guild_update(before: discord.Guild, after: discord.Guild):
    embed = discord.Embed(title="Server Updated", color=discord.Color.orange())
    embed.add_field(name="Server", value=f"{after.name} ({after.id})", inline=False)
    await log_embed(after, "server_log_channel", embed)

@bot.event
async def on_guild_role_create(role: discord.Role):
    actor, reason = await _find_actor(role.guild, discord.AuditLogAction.role_create, role.id)
    embed = discord.Embed(title="Role Created", color=discord.Color.green())
    embed.add_field(name="Role", value=f"{role.name} ({role.id})", inline=False)
    if actor:
        embed.add_field(name="Actor", value=f"{actor} ({actor.id})", inline=False)
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    await log_embed(role.guild, "server_log_channel", embed)

@bot.event
async def on_guild_role_delete(role: discord.Role):
    actor, reason = await _find_actor(role.guild, discord.AuditLogAction.role_delete, role.id)
    embed = discord.Embed(title="Role Deleted", color=discord.Color.dark_red())
    embed.add_field(name="Role", value=f"{role.name} ({role.id})", inline=False)
    if actor:
        embed.add_field(name="Actor", value=f"{actor} ({actor.id})", inline=False)
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    await log_embed(role.guild, "server_log_channel", embed)

@bot.event
async def on_guild_role_update(before: discord.Role, after: discord.Role):
    actor, reason = await _find_actor(after.guild, discord.AuditLogAction.role_update, after.id)
    embed = discord.Embed(title="Role Updated", color=discord.Color.orange())
    embed.add_field(name="Role", value=f"{after.name} ({after.id})", inline=False)
    if before.name != after.name:
        embed.add_field(name="Name", value=f"{before.name} → {after.name}", inline=False)
    if before.permissions != after.permissions:
        embed.add_field(name="Permissions", value="Changed", inline=False)
    if actor:
        embed.add_field(name="Actor", value=f"{actor} ({actor.id})", inline=False)
    if reason:
        embed.add_field(name="Reason", value=reason, inline=False)
    await log_embed(after.guild, "server_log_channel", embed)

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if not member.guild:
        return
    embed = discord.Embed(title="Voice Update", color=discord.Color.blurple())
    embed.add_field(name="User", value=f"{member} ({member.id})", inline=False)
    if not before.channel and after.channel:
        embed.add_field(name="Action", value=f"Joined {after.channel.name}", inline=False)
    elif before.channel and not after.channel:
        embed.add_field(name="Action", value=f"Left {before.channel.name}", inline=False)
    elif before.channel and after.channel and before.channel.id != after.channel.id:
        embed.add_field(name="Action", value=f"Switched {before.channel.name} → {after.channel.name}", inline=False)
    if before.mute != after.mute:
        embed.add_field(name="Mute", value=str(after.mute), inline=False)
    if before.deaf != after.deaf:
        embed.add_field(name="Deaf", value=str(after.deaf), inline=False)
    await log_embed(member.guild, "voice_log_channel", embed)
async def get_guild_assets(guild_id):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT custom_assets_json FROM guild_config WHERE guild_id = ?', (int(guild_id),)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                try:
                    custom = json.loads(row[0])
                    if isinstance(custom, dict):
                        fixed = {}
                        for key, data in custom.items():
                            try:
                                price = int(data.get("price", 0))
                                income = int(data.get("income", 0))
                            except Exception:
                                continue
                            if price <= 0:
                                continue
                            if income < 0:
                                income = 0
                            max_income = price * 20
                            if income > max_income:
                                income = max_income
                            fixed[key] = {
                                "name": data.get("name", key),
                                "price": price,
                                "income": income
                            }
                        return {**DEFAULT_ASSETS, **fixed}
                except json.JSONDecodeError:
                    return DEFAULT_ASSETS
    return DEFAULT_ASSETS

async def get_guild_banks(guild_id):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT bank_plans_json FROM guild_config WHERE guild_id = ?', (int(guild_id),)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                try:
                    data = json.loads(row[0])
                    if isinstance(data, dict) and data:
                        fixed = {}
                        for key, info in data.items():
                            try:
                                price = int(info.get("price", 0))
                            except Exception:
                                price = 0
                            steps = max(0, price // 50000)
                            allowed_min_pct = 1 + steps * 1
                            allowed_max_pct = 2 + steps * 2
                            try:
                                min_rate = float(info.get("min", 0.01))
                                max_rate = float(info.get("max", 0.02))
                            except Exception:
                                min_rate = 0.01
                                max_rate = 0.02
                            min_pct = max(0.0, min_rate * 100.0)
                            max_pct = max(0.0, max_rate * 100.0)
                            if min_pct > allowed_min_pct:
                                min_pct = allowed_min_pct
                            if max_pct > allowed_max_pct:
                                max_pct = allowed_max_pct
                            if max_pct < min_pct:
                                max_pct = min_pct
                            fixed[key] = {
                                "name": info.get("name", key),
                                "min": min_pct / 100.0,
                                "max": max_pct / 100.0,
                                "price": price,
                                "min_level": int(info.get("min_level", 0))
                            }
                        if fixed:
                            return fixed
                except json.JSONDecodeError:
                    pass
    return DEFAULT_BANK_PLANS

def compute_boost_multiplier(level):
    return min(2.0, 1.25 + (level * 0.05))

async def ensure_wonder(guild_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR IGNORE INTO guild_wonder (guild_id) VALUES (?)', (guild_id,))
        await db.commit()

async def get_wonder(guild_id):
    await ensure_wonder(guild_id)
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM guild_wonder WHERE guild_id = ?', (guild_id,)) as cursor:
            return await cursor.fetchone()

async def get_user_job(user_id, guild_id):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT job_id FROM user_jobs WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

def get_server_join_multiplier(user_id):
    if not SUPPORT_GUILD_ID:
        return 1.0
    guild = bot.get_guild(SUPPORT_GUILD_ID)
    if not guild:
        return 1.0
    member = guild.get_member(user_id)
    return 2.0 if member else 1.0

async def ensure_quest_resets(user_id, guild_id):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT daily_reset, weekly_reset, daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_quest_completed_json, weekly_quest_completed_json, daily_stats_json, weekly_stats_json FROM users WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return
        daily_reset, weekly_reset, daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_completed_json, weekly_completed_json, daily_stats_json, weekly_stats_json = row
        changed = False
        if daily_reset is None or daily_reset == 0 or now - daily_reset >= 86400:
            daily_reset = now
            daily_commands = 0
            daily_reward_claimed = 0
            daily_completed_json = '{}'
            daily_stats_json = '{}'
            changed = True
        if weekly_reset is None or weekly_reset == 0 or now - weekly_reset >= 604800:
            weekly_reset = now
            weekly_commands = 0
            weekly_reward_claimed = 0
            weekly_completed_json = '{}'
            weekly_stats_json = '{}'
            changed = True
        if changed:
            await db.execute('UPDATE users SET daily_reset = ?, weekly_reset = ?, daily_commands = ?, weekly_commands = ?, daily_reward_claimed = ?, weekly_reward_claimed = ?, daily_quest_completed_json = ?, weekly_quest_completed_json = ?, daily_stats_json = ?, weekly_stats_json = ? WHERE user_id = ? AND guild_id = ?', (daily_reset, weekly_reset, daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_completed_json, weekly_completed_json, daily_stats_json, weekly_stats_json, user_id, guild_id))
            await db.commit()

async def increment_stat(user_id, guild_id, key):
    await ensure_quest_resets(user_id, guild_id)
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT daily_stats_json, weekly_stats_json FROM users WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return
        try:
            daily_stats = json.loads(row[0] or '{}')
        except:
            daily_stats = {}
        try:
            weekly_stats = json.loads(row[1] or '{}')
        except:
            weekly_stats = {}
        daily_stats[key] = int(daily_stats.get(key, 0)) + 1
        weekly_stats[key] = int(weekly_stats.get(key, 0)) + 1
        await db.execute('UPDATE users SET daily_stats_json = ?, weekly_stats_json = ? WHERE user_id = ? AND guild_id = ?', (json.dumps(daily_stats), json.dumps(weekly_stats), user_id, guild_id))
        await db.commit()

async def increment_quests(user_id, guild_id, command_name=None):
    await ensure_quest_resets(user_id, guild_id)
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_quest_completed_json, weekly_quest_completed_json, daily_stats_json, weekly_stats_json FROM users WHERE user_id = ? AND guild_id = ?', (user_id, guild_id)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return
        daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_completed_json, weekly_completed_json, daily_stats_json, weekly_stats_json = row
        try:
            daily_completed = json.loads(daily_completed_json) if daily_completed_json else {}
        except:
            daily_completed = {}
        try:
            weekly_completed = json.loads(weekly_completed_json) if weekly_completed_json else {}
        except:
            weekly_completed = {}
        try:
            daily_stats = json.loads(daily_stats_json) if daily_stats_json else {}
        except:
            daily_stats = {}
        try:
            weekly_stats = json.loads(weekly_stats_json) if weekly_stats_json else {}
        except:
            weekly_stats = {}
        daily_commands += 1
        weekly_commands += 1
        kinds = ["commands"]
        if command_name:
            name = command_name.lower()
            if name == "work":
                kinds.append("work")
            if name == "crime":
                kinds.append("crime")
            if name in ["blackjack", "roulette"]:
                kinds.append("gamble")
        for k in kinds:
            daily_stats[k] = int(daily_stats.get(k, 0)) + 1
            weekly_stats[k] = int(weekly_stats.get(k, 0)) + 1
        _econ = EconomyService()
        daily_active = _econ._pick_daily(guild_id, now)
        weekly_active = _econ._pick_weekly(guild_id, now)
        reward_balance_changes = 0
        for quest in daily_active:
            qid = quest["id"]
            if not daily_completed.get(qid):
                kind = quest.get("kind", "commands")
                if kind == "commands":
                    progress_val = daily_commands
                else:
                    progress_val = int(daily_stats.get(kind, 0))
                if progress_val >= quest["target"]:
                    reward_balance_changes += quest["reward"]
                    daily_completed[qid] = True
        for quest in weekly_active:
            qid = quest["id"]
            if not weekly_completed.get(qid):
                kind = quest.get("kind", "commands")
                if kind == "commands":
                    progress_val = weekly_commands
                else:
                    progress_val = int(weekly_stats.get(kind, 0))
                if progress_val >= quest["target"]:
                    reward_balance_changes += quest["reward"]
                    weekly_completed[qid] = True
        daily_completed_json = json.dumps(daily_completed)
        weekly_completed_json = json.dumps(weekly_completed)
        daily_stats_json = json.dumps(daily_stats)
        weekly_stats_json = json.dumps(weekly_stats)
        await db.execute('UPDATE users SET daily_commands = ?, weekly_commands = ?, daily_reward_claimed = ?, weekly_reward_claimed = ?, daily_quest_completed_json = ?, weekly_quest_completed_json = ?, daily_stats_json = ?, weekly_stats_json = ? WHERE user_id = ? AND guild_id = ?', (daily_commands, weekly_commands, daily_reward_claimed, weekly_reward_claimed, daily_completed_json, weekly_completed_json, daily_stats_json, weekly_stats_json, user_id, guild_id))
        if reward_balance_changes > 0:
            await db.execute('UPDATE users SET balance = balance + ? WHERE user_id = ? AND guild_id = ?', (reward_balance_changes, user_id, guild_id))
        await db.commit()

 

# --- Tasks ---
@tasks.loop(minutes=10)
async def passive_income_task():
    async with aiosqlite.connect(DB_FILE) as db:
        # Fetch all assets and user data in one go to handle auto-deposit and income
        async with db.execute('''
            SELECT ua.user_id, ua.guild_id, ua.asset_id, ua.count, u.auto_deposit, u.last_vote
            FROM user_assets ua
            JOIN users u ON ua.user_id = u.user_id AND ua.guild_id = u.guild_id
            WHERE ua.count > 0
        ''') as cursor:
            rows = await cursor.fetchall()
        
        if not rows: return

        now = int(time.time())
        # Group by guild to fetch configs once
        guild_groups = {}
        for uid, gid, aid, count, auto_dep, last_vote in rows:
            if gid not in guild_groups: guild_groups[gid] = []
            guild_groups[gid].append((uid, aid, count, auto_dep, last_vote))

        updates_balance = [] # List of (income, uid, gid)
        updates_bank = []    # List of (income, uid, gid)
        updates_passive = [] # List of (income, uid, gid)

        for gid, members in guild_groups.items():
            assets_config = await get_guild_assets(gid)
            await db.execute('INSERT OR IGNORE INTO guild_wonder (guild_id) VALUES (?)', (gid,))
            async with db.execute('SELECT boost_multiplier, boost_until FROM guild_wonder WHERE guild_id = ?', (gid,)) as cursor:
                wonder_row = await cursor.fetchone()
            boost_multiplier = 1.0
            if wonder_row:
                boost_multiplier = wonder_row[0] if now < wonder_row[1] else 1.0
            user_data = {} # uid -> {'income': 0, 'auto_dep': 0, 'last_vote': 0}
            
            for uid, aid, count, auto_dep, last_vote in members:
                if aid in assets_config:
                    income = assets_config[aid]['income'] * count
                    if uid not in user_data:
                        user_data[uid] = {'income': 0, 'auto_dep': auto_dep, 'last_vote': last_vote}
                    user_data[uid]['income'] += income
            
            for uid, data in user_data.items():
                if data['income'] > 0:
                    # Check if auto-deposit is active (voted in last 12 hours)
                    is_voter = (now - data['last_vote']) < 43200 # 12 hours
                    adjusted_income = int(data['income'] * boost_multiplier)
                    updates_passive.append((data['income'], uid, gid))
                    if data['auto_dep'] and is_voter:
                        updates_bank.append((adjusted_income, uid, gid))
                    else:
                        updates_balance.append((adjusted_income, uid, gid))

        if updates_balance:
            await db.executemany('UPDATE users SET balance = balance + ? WHERE user_id = ? AND guild_id = ?', updates_balance)
        if updates_bank:
            await db.executemany('UPDATE users SET bank = bank + ? WHERE user_id = ? AND guild_id = ?', updates_bank)
        if updates_passive:
            await db.executemany('UPDATE users SET passive_income = ? WHERE user_id = ? AND guild_id = ?', updates_passive)
        
        await db.commit()

@tasks.loop(hours=1)
async def leaderboard_rewards_task():
    """Update top 3 multipliers and titles hourly."""
    categories = {
        "commands": 'SELECT user_id, SUM(total_commands) as total FROM users GROUP BY user_id ORDER BY total DESC LIMIT 3',
        "robs": 'SELECT user_id, SUM(successful_robs) as total FROM users GROUP BY user_id ORDER BY total DESC LIMIT 3',
        "crimes": 'SELECT user_id, SUM(successful_crimes) as total FROM users GROUP BY user_id ORDER BY total DESC LIMIT 3',
        "money": 'SELECT user_id, SUM(balance + bank) as total FROM users GROUP BY user_id ORDER BY total DESC LIMIT 3',
        "passive": 'SELECT user_id, SUM(passive_income) as total FROM users GROUP BY user_id ORDER BY total DESC LIMIT 3',
        "level": 'SELECT user_id, MAX(level) as total FROM users GROUP BY user_id ORDER BY total DESC LIMIT 3'
    }
    
    titles_map = {
        "commands": ["Command Master", "Command Expert", "Command Enthusiast"],
        "robs": ["Master Thief", "Elite Robber", "Pickpocket"],
        "crimes": ["Godfather", "Crime Lord", "Thug"],
        "money": ["Emperor", "Tycoon", "Wealthy Merchant"],
        "passive": ["Industrialist", "Business Mogul", "Investor"],
        "level": ["Grand Sage", "Wise Elder", "Scholar"]
    }

    # Reset current leaderboard multipliers for all users in memory or just track who changed?
    # Simpler: Clear all 'lb_' multipliers and re-assign.
    async with aiosqlite.connect(DB_FILE) as db:
        try:
            await db.execute('PRAGMA journal_mode=WAL')
            await db.execute('PRAGMA busy_timeout=5000')
        except:
            pass
        # Get all users with lb_ multipliers
        async with db.execute("SELECT user_id, multipliers_json, titles_json, medals_json FROM user_rewards") as cursor:
            rows = await cursor.fetchall()
            
        for uid, mults_json, titles_json, medals_json in rows:
            mults = json.loads(mults_json)
            titles = json.loads(titles_json)
            medals = json.loads(medals_json)
            
            # Remove existing lb_ mults, titles, and medals
            mults = {k: v for k, v in mults.items() if not k.startswith('lb_')}
            titles = [t for t in titles if not t.get('source', '').startswith('lb_')]
            medals = [m for m in medals if not m.get('source', '').startswith('lb_')]
            
            await db.execute("UPDATE user_rewards SET multipliers_json = ?, titles_json = ?, medals_json = ? WHERE user_id = ?", 
                            (json.dumps(mults), json.dumps(titles), json.dumps(medals), uid))
        await db.commit()

        # Re-assign based on current top 3
        for cat_id, query in categories.items():
            async with db.execute(query) as cursor:
                top_rows = await cursor.fetchall()
                
            for i, row in enumerate(top_rows):
                uid = row[0]
                rank = i + 1
                multiplier = 2.0 if rank == 1 else 1.5 if rank == 2 else 1.25
                medal_emoji = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉"
                title_name = titles_map[cat_id][i]
                
                await ensure_rewards(uid, db=db)
                async with db.execute("SELECT multipliers_json, titles_json, medals_json FROM user_rewards WHERE user_id = ?", (uid,)) as cursor:
                    r = await cursor.fetchone()
                    mults = json.loads(r[0])
                    titles = json.loads(r[1])
                    medals = json.loads(r[2])
                
                mults[f"lb_{cat_id}"] = multiplier
                titles.append({"title": title_name, "source": f"lb_{cat_id}", "timestamp": int(time.time())})
                medals.append({"medal": medal_emoji, "source": f"lb_{cat_id}", "timestamp": int(time.time())})
                
                await db.execute("UPDATE user_rewards SET multipliers_json = ?, titles_json = ?, medals_json = ? WHERE user_id = ?", 
                                (json.dumps(mults), json.dumps(titles), json.dumps(medals), uid))
        await db.commit()

@tasks.loop(hours=1)
async def interest_task():
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT user_id, guild_id, bank, bank_plan FROM users WHERE bank > 0') as cursor:
            rows = await cursor.fetchall()
        if not rows:
            return
        updates = []
        for uid, gid, bank, plan in rows:
            plan_id = plan or 'standard'
            if gid == 0:
                plan_data = DEFAULT_BANK_PLANS.get(plan_id) or DEFAULT_BANK_PLANS.get('standard')
            else:
                banks_config = await get_guild_banks(gid)
                plan_data = banks_config.get(plan_id) or banks_config.get('standard')
            rate_min = float((plan_data or {}).get('min', 0.01))
            rate_max = float((plan_data or {}).get('max', 0.02))
            interest = int(bank * random.uniform(rate_min, rate_max))
            if interest > 0:
                updates.append((interest, uid, gid))
        if updates:
            await db.executemany('UPDATE users SET bank = bank + ? WHERE user_id = ? AND guild_id = ?', updates)
        await db.commit()

@tasks.loop(minutes=5)
async def vote_reminder_task():
    """Check for users whose vote expired in the last 5 minutes and notify them."""
    now = int(time.time())
    twelve_hours_ago = now - 43200
    
    async with aiosqlite.connect(DB_FILE) as db:
        # Find users who voted exactly 12h (+/- 5 mins) ago
        async with db.execute('''
            SELECT DISTINCT user_id FROM users 
            WHERE last_vote > ? AND last_vote <= ?
        ''', (twelve_hours_ago - 300, twelve_hours_ago)) as cursor:
            rows = await cursor.fetchall()
            
    for row in rows:
        user_id = row[0]
        try:
            user = await bot.fetch_user(user_id)
            if user:
                vote_url = f"https://top.gg/bot/{bot.user.id}/vote"
                embed = discord.Embed(title="⌛ Vote Expired!", color=0xffa500)
                embed.description = f"Your 12-hour vote rewards for **Empire Nexus** have expired!\n\n" \
                                    f"Vote again now to keep your **Auto-Deposit** active and support the bot!\n\n" \
                                    f"[**Click here to revote on Top.gg**]({vote_url})"
                await user.send(embed=embed)
        except:
            pass # User might have DMs closed

@tasks.loop(minutes=30)
async def update_topgg_stats():
    """Automatically update the bot's server count on Top.gg."""
    topgg_token = os.getenv('TOPGG_TOKEN')
    if not topgg_token:
        return
    
    url = f"https://top.gg/api/bots/{bot.user.id}/stats"
    headers = {"Authorization": topgg_token}
    payload = {"server_count": len(bot.guilds)}
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status == 200:
                    print(f"DEBUG: Successfully updated Top.gg server count to {len(bot.guilds)}")
                else:
                    print(f"DEBUG: Failed to update Top.gg stats: {resp.status}")
        except Exception as e:
            print(f"DEBUG: Error updating Top.gg stats: {e}")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        minutes, seconds = divmod(error.retry_after, 60)
        return await ctx.send(f"⏳ **Cooldown!** Try again in **{int(minutes)}m {int(seconds)}s**.", delete_after=10)
    elif isinstance(error, commands.MissingPermissions):
        return await ctx.send("❌ You don't have permission to use this command!")
    elif isinstance(error, commands.BadArgument):
        return await ctx.send("❌ Invalid argument provided! Check `.help`.")
    print(f"DEBUG Error: {error}")

@bot.event
async def on_command_completion(ctx):
    if ctx.guild is None:
        return
    leveled_up, new_level = await add_xp(ctx.author.id, ctx.guild.id, 5)
    
    # Track global command count for leaderboards
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('UPDATE users SET total_commands = total_commands + 1 WHERE user_id = ? AND guild_id = ?', 
                        (ctx.author.id, ctx.guild.id))
        await db.commit()

    cmd_name = ctx.command.name if ctx.command else None
    await increment_quests(ctx.author.id, ctx.guild.id, cmd_name)
    if leveled_up:
        await ctx.send(f"🎊 **LEVEL UP!** {ctx.author.mention} reached **Level {new_level}**!")

@bot.event
async def on_ready():
    await init_db()
    await migrate_db()
    ok = await ensure_single_instance()
    if not ok:
        try:
            await bot.close()
        except:
            pass
        return
    if not _acquire_file_lock():
        try:
            await bot.close()
        except:
            pass
        return
    
    global SUPPORT_GUILD_ID
    global PROMO_TASK_STARTED
    try:
        invite = await bot.fetch_invite(SUPPORT_SERVER_INVITE)
        if invite.guild:
            SUPPORT_GUILD_ID = invite.guild.id
            print(f"DEBUG: Resolved Support Guild ID: {SUPPORT_GUILD_ID}")
    except Exception as e:
        print(f"DEBUG: Could not resolve support invite: {e}")

    try:
        boss_service = BossService(bot, LOOT_ITEMS, BOSSES)
        setup_boss_commands(bot, boss_service)
        setup_quests_commands(bot)
        setup_profile_commands(bot)
        setup_assets_commands(bot)
        setup_economy_commands(bot)
    except:
        pass

    try:
        synced = await bot.tree.sync()
        print(f"DEBUG: Synced {len(synced)} global slash commands.")
    except Exception as e:
        print(f"CRITICAL: Error syncing slash commands: {e}")

    interest_task.start()
    leaderboard_rewards_task.start()
    passive_income_task.start()
    vote_reminder_task.start()
    update_topgg_stats.start()
    try:
        if not PROMO_TASK_STARTED:
            asyncio.create_task(_promotion_loop())
            PROMO_TASK_STARTED = True
    except Exception:
        PROMO_TASK_STARTED = True
    try:
        instance_heartbeat_task.start()
    except:
        pass
    try:
        now = int(time.time())
        async with aiosqlite.connect(DB_FILE) as db:
            for g in bot.guilds:
                await db.execute('INSERT OR IGNORE INTO bot_guilds (guild_id, first_seen) VALUES (?, ?)', (g.id, now))
            await db.commit()
    except Exception as e:
        print(f"CRITICAL: Error syncing guild commands: {e}")
    print(f'Logged in as {bot.user.name}')
    try:
        bw = BossWorker(bot, boss_service)
        t = bw.start()
        if t:
            _task_manager.register(t)
    except:
        pass
    await _clear_guild_commands_once()

@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        pass
        now = int(time.time())
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR IGNORE INTO bot_guilds (guild_id, first_seen) VALUES (?, ?)', (guild.id, now))
            await db.commit()
    except Exception:
        pass
@bot.event
async def on_member_join(member):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT welcome_channel, welcome_message, welcome_embed_json FROM welcome_farewell WHERE guild_id = ?', (member.guild.id,)) as cursor:
            wf = await cursor.fetchone()

    if wf and wf[0]:
        ch = await resolve_channel(member.guild, wf[0])
        if ch:
            placeholders = _apply_placeholders_member(member.guild, member)
            embed_to_send = None
            msg_to_send = None
            embed_json = wf[2]
            if embed_json:
                try:
                    data = json.loads(embed_json)
                    data = _replace_in_data(member.guild, data, placeholders)
                    embed_to_send = discord.Embed.from_dict(data)
                except:
                    embed_to_send = None
            if not embed_to_send:
                msg = _resolve_text_mentions(member.guild, (wf[1] or "").strip())
                if msg:
                    for k, v in placeholders.items(): msg = msg.replace(k, v)
                    msg_to_send = msg
                else:
                    embed_to_send = discord.Embed(
                        title=f"👋 Welcome {member.name}",
                        description=f"Glad to have you in {member.guild.name}! You are member #{member.guild.member_count}.",
                        color=0x00d2ff,
                        timestamp=discord.utils.utcnow()
                    )
                    try: embed_to_send.set_thumbnail(url=member.display_avatar.url)
                    except: pass
            try:
                if embed_to_send:
                    await ch.send(embed=embed_to_send)
                else:
                    await ch.send(content=msg_to_send)
            except:
                pass
    # Auto-assign configured role
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT role_id FROM guild_auto_role WHERE guild_id = ?', (member.guild.id,)) as c:
                r = await c.fetchone()
        if r and r[0]:
            role = member.guild.get_role(int(r[0]))
            if role:
                try:
                    await member.add_roles(role, reason="Auto-assign on join")
                except:
                    pass
    except:
        pass

    account_age = (discord.utils.utcnow() - member.created_at).days
    join_embed = discord.Embed(title="📥 Member Joined", color=discord.Color.green(), timestamp=discord.utils.utcnow())
    join_embed.set_thumbnail(url=member.display_avatar.url)
    join_embed.add_field(name="User", value=f"{member.mention} ({member.id})")
    join_embed.add_field(name="Account Age", value=f"{account_age} days")
    if account_age < 7:
        join_embed.description = "⚠️ New Account"
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT join_log_channel, member_log_channel FROM logging_config WHERE guild_id = ?', (member.guild.id,)) as cursor:
            row = await cursor.fetchone()
    if row and row[0]:
        await log_embed(member.guild, "join_log_channel", join_embed)
    elif row and row[1]:
        await log_embed(member.guild, "member_log_channel", join_embed)

@bot.event
async def on_member_remove(member):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT farewell_channel, farewell_message, farewell_embed_json FROM welcome_farewell WHERE guild_id = ?', (member.guild.id,)) as cursor:
            wf = await cursor.fetchone()

    if wf and wf[0]:
        ch = await resolve_channel(member.guild, wf[0])
        if ch:
            placeholders = _apply_placeholders_member(member.guild, member)
            embed_to_send = None
            msg_to_send = None
            embed_json = wf[2]
            if embed_json:
                try:
                    data = json.loads(embed_json)
                    data = _replace_in_data(member.guild, data, placeholders)
                    embed_to_send = discord.Embed.from_dict(data)
                except:
                    embed_to_send = None
            if not embed_to_send:
                msg = _resolve_text_mentions(member.guild, (wf[1] or "").strip())
                if msg:
                    for k, v in placeholders.items(): msg = msg.replace(k, v)
                    msg_to_send = msg
            try:
                if embed_to_send:
                    try:
                        if not embed_to_send.thumbnail.url:
                            embed_to_send.set_thumbnail(url=member.display_avatar.url)
                    except:
                        try: embed_to_send.set_thumbnail(url=member.display_avatar.url)
                        except: pass
                    await ch.send(embed=embed_to_send)
                elif msg_to_send:
                    await ch.send(content=msg_to_send)
            except:
                pass

    leave_embed = discord.Embed(title="📤 Member Left", color=discord.Color.orange(), timestamp=discord.utils.utcnow())
    leave_embed.set_thumbnail(url=member.display_avatar.url)
    leave_embed.add_field(name="User", value=f"{member.mention} ({member.id})")
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT leave_log_channel, member_log_channel FROM logging_config WHERE guild_id = ?', (member.guild.id,)) as cursor:
            row = await cursor.fetchone()
    if row and row[0]:
        await log_embed(member.guild, "leave_log_channel", leave_embed)
    elif row and row[1]:
        await log_embed(member.guild, "member_log_channel", leave_embed)

@bot.event
async def on_message_delete(message):
    if not message.guild or message.author.bot: return
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT message_log_channel FROM logging_config WHERE guild_id = ?', (message.guild.id,)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                channel = await resolve_channel(message.guild, row[0])
                if channel:
                    embed = discord.Embed(title="Message Deleted", color=discord.Color.red())
                    embed.add_field(name="Author", value=f"{message.author} ({message.author.id})")
                    embed.add_field(name="Channel", value=message.channel.mention)
                    embed.add_field(name="Content", value=message.content or "[No content]", inline=False)
                    embed.timestamp = discord.utils.utcnow()
                    await channel.send(embed=embed)

@bot.event
async def on_message_edit(before, after):
    if not before.guild or before.author.bot: return
    if before.content == after.content: return
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT message_log_channel FROM logging_config WHERE guild_id = ?', (before.guild.id,)) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                channel = await resolve_channel(before.guild, row[0])
                if channel:
                    embed = discord.Embed(title="Message Edited", color=discord.Color.blue())
                    embed.add_field(name="Author", value=f"{before.author} ({before.author.id})")
                    embed.add_field(name="Channel", value=before.channel.mention)
                    embed.add_field(name="Before", value=before.content or "[No content]", inline=False)
                    embed.add_field(name="After", value=after.content or "[No content]", inline=False)
                    embed.timestamp = discord.utils.utcnow()
                    await channel.send(embed=embed)

@bot.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    if not payload.guild_id:
        return
    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    # Build a minimal embed; content is unavailable for uncached deletes
    embed = discord.Embed(title="Message Deleted (Uncached)", color=discord.Color.red())
    embed.add_field(name="Channel ID", value=str(payload.channel_id), inline=True)
    embed.add_field(name="Message ID", value=str(payload.message_id), inline=True)
    if payload.cached_message:
        author = payload.cached_message.author
        embed.add_field(name="Author", value=f"{author} ({author.id})", inline=False)
        embed.add_field(name="Content", value=payload.cached_message.content or "[No content]", inline=False)
    embed.timestamp = discord.utils.utcnow()
    await log_embed(guild, "message_log_channel", embed)

@bot.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
    return
@bot.event
async def on_raw_reaction_add(payload):
    if payload.user_id == bot.user.id: return
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT role_id FROM reaction_roles WHERE message_id = ? AND emoji = ?', (payload.message_id, str(payload.emoji)) ) as cursor:
            row = await cursor.fetchone()
            if row:
                guild = bot.get_guild(payload.guild_id)
                role = guild.get_role(row[0])
                member = guild.get_member(payload.user_id)
                if role and member:
                    try:
                        await member.add_roles(role)
                    except:
                        pass

@bot.event
async def on_raw_reaction_remove(payload):
    if payload.user_id == bot.user.id: return
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT role_id FROM reaction_roles WHERE message_id = ? AND emoji = ?', (payload.message_id, str(payload.emoji)) ) as cursor:
            row = await cursor.fetchone()
            if row:
                guild = bot.get_guild(payload.guild_id)
                role = guild.get_role(row[0])
                member = guild.get_member(payload.user_id)
                if role and member:
                    try:
                        await member.remove_roles(role)
                    except:
                        pass

# --- Hybrid Commands ---

@bot.event
async def on_message(message):
    if not message.guild or message.author.bot:
        return
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT raid_mode, anti_phish_enabled FROM guild_config WHERE guild_id = ?', (message.guild.id,)) as cursor:
                row = await cursor.fetchone()
        raid_mode = int(row[0] or 0) if row else 0
        anti_phish = int(row[1] or 1) if row else 1
    except:
        raid_mode = 0
        anti_phish = 1
    if raid_mode == 1:
        perms = message.author.guild_permissions
        if not perms.manage_messages and not perms.administrator:
            try:
                await message.delete()
            except:
                pass
            embed = discord.Embed(title="Raid Mode", description=f"Blocked a message from {message.author.mention} in {message.channel.mention}.", color=discord.Color.red(), timestamp=discord.utils.utcnow())
            await log_embed(message.guild, "automod_log_channel", embed)
            return
    if anti_phish == 1:
        content = (message.content or "").lower()
        suspect = any(x in content for x in ["free nitro","discordgift","nitro-gift","airdrop","giveaway","steamcommunity"])
        if suspect:
            try:
                await message.delete()
            except:
                pass
            embed = discord.Embed(title="Anti‑Phishing", description=f"Removed a suspicious message from {message.author.mention}.", color=discord.Color.orange(), timestamp=discord.utils.utcnow())
            embed.add_field(name="Channel", value=message.channel.mention, inline=True)
            await log_embed(message.guild, "automod_log_channel", embed)
            return
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR IGNORE INTO mod_stats (user_id, guild_id) VALUES (?, ?)', (message.author.id, message.guild.id))
            await db.execute('UPDATE mod_stats SET messages = messages + 1, points = points + 1 WHERE user_id = ? AND guild_id = ?', (message.author.id, message.guild.id))
            await db.commit()
    except:
        pass
    await bot.process_commands(message)
@bot.hybrid_command(name="start", description="New to the Empire? Start your tutorial here!")
async def start_tutorial(ctx: commands.Context):
    data = await get_user_data(ctx.author.id, ctx.guild.id)
    msg_bonus = ""
    
    # Check if 'started' is 0 or None (handle case where column was just added so it might be 0)
    # The default is 0.
    if not data['started']:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('UPDATE users SET balance = balance + 500, started = 1 WHERE user_id = ? AND guild_id = ?', (ctx.author.id, ctx.guild.id))
            await db.commit()
        msg_bonus = "\n\n🎉 **Welcome Bonus!** You received **500 coins** for starting your journey!"

    embed = discord.Embed(
        title="🌅 Welcome to Empire Nexus",
        description=(
            f"You have inherited a small plot of land and 100 coins. Your goal: **Build the wealthiest empire in the server.**{msg_bonus}\n\n"
            "**Step 1: Get Started**\n"
            "Use `.work` or `/work` to supervise the mines and earn your first coins.\n\n"
            "**Step 2: Invest Wisely**\n"
            "Visit the `.shop` and buy your first **Lemonade Stand**. It will generate income for you every 10 minutes, even while you sleep!\n\n"
            "**Step 3: Secure Your Wealth**\n"
            "Other rulers can `.rob` you! Use `.deposit <amount>` (or `.dep`) to move your coins into the **Bank**. Banked coins are safe from thieves and earn **hourly interest**.\n\n"
            "**Step 4: Expand & Conquer**\n"
            "Once you reach Level 10, you can `.prestige` to reset your progress for a permanent income bonus.\n\n"
            "**Need more help?**\n"
            "Type `.help` for a full command list or `.setup` to configure your server's dashboard."
        ),
        color=0x00d2ff
    )
    embed.set_thumbnail(url=bot.user.display_avatar.url)
    embed.set_footer(text="Your journey to greatness begins now.")
    await ctx.send(embed=apply_theme(embed))

def apply_theme(embed: discord.Embed) -> discord.Embed:
    try:
        if embed.color is None or embed.color.value == 0:
            embed.color = discord.Color(0x00d2ff)
    except:
        pass
    try:
        if not getattr(embed, "timestamp", None):
            embed.timestamp = discord.utils.utcnow()
    except:
        pass
    try:
        embed.set_footer(text="Empire Nexus")
    except:
        pass
    return embed

class HelpSelect(discord.ui.Select):
    def __init__(self, prefix, show_owner):
        self.prefix = prefix
        self.show_owner = show_owner
        options = [
            discord.SelectOption(label="Making Money", description="Work, crime, gambling, and jobs", emoji="💸"),
            discord.SelectOption(label="Banking", description="Deposit, withdraw, and bank plans", emoji="🏦"),
            discord.SelectOption(label="Assets & Empire", description="Shop, inventory, and prestige", emoji="🏗️"),
            discord.SelectOption(label="Wonder & Server Progress", description="Server-wide projects and boosts", emoji="🏛️"),
            discord.SelectOption(label="Boosters & Rewards", description="Voting and support server bonuses", emoji="🚀"),
            discord.SelectOption(label="Moderation", description="Kick, ban, warns, automod", emoji="🛡️"),
            discord.SelectOption(label="Utility & Info", description="Ping, serverinfo, userinfo, avatar", emoji="🧭"),
            discord.SelectOption(label="Welcome & Config", description="Welcome, farewell, setprefix, setlogs", emoji="📑"),
            discord.SelectOption(label="Setup & Utility", description="Help, settings, and tutorial", emoji="⚙️")
        ]
        if self.show_owner:
            options.insert(8, discord.SelectOption(label="Owner & Admin", description="Owner-only economy management", emoji="👑"))
        super().__init__(placeholder="Select a category to view its commands!", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        try:
            category_map = {
                "Making Money": "making money",
                "Banking": "banking",
                "Assets & Empire": "assets",
                "Wonder & Server Progress": "wonder",
                "Boosters & Rewards": "boosters",
                "Moderation": "moderation",
                "Utility & Info": "info",
                "Welcome & Config": "welcome",
                "Owner & Admin": "owner",
                "Setup & Utility": "utility"
            }
            selected_label = self.values[0]
            key = category_map.get(selected_label)
            prefix = self.prefix

            categories = {
                "making money": {
                    "title": "💸 Making Money",
                    "commands": [
                        f"`{prefix}work`, `/work` – Supervise mines for coins.",
                        f"`{prefix}crime`, `/crime` – High risk, high reward heists.",
                        f"`{prefix}blackjack`, `/blackjack` – Casino blackjack.",
                        f"`{prefix}roulette`, `/roulette` – Spin the wheel.",
                        f"`{prefix}riddle`, `/riddle` and `{prefix}answer` – Solve riddles.",
                        f"`{prefix}jobs`, `/jobs` – View available jobs.",
                        f"`{prefix}applyjob <id>`, `/applyjob` – Apply for a job.",
                        f"`{prefix}dailyquests`, `/dailyquests` – Daily quest checklist.",
                        f"`{prefix}weeklyquests`, `/weeklyquests` – Weekly quest checklist."
                    ],
                    "explain": (
                        "Use work, crime, and the casino commands to generate coins. "
                        "Pick a job with jobs/applyjob to boost income from your favourite activity. "
                        "Daily and weekly quests reward consistent play; stack activities while quests are active."
                    )
                },
                "banking": {
                    "title": "🏦 Banking",
                    "commands": [
                        f"`{prefix}deposit <amount>`, `/deposit` – Move coins into the bank.",
                        f"`{prefix}withdraw <amount>`, `/withdraw` – Take coins out of the bank.",
                        f"`{prefix}balance`, `/balance` – View wallet, bank and bank plan.",
                        f"`{prefix}bank`, `/bank` – View and switch bank plans.",
                        f"`{prefix}autodeposit`, `/autodeposit` – Auto‑deposit passive income (with vote).",
                        f"`{prefix}vote`, `/vote` – Vote for Top.gg rewards.",
                        f"`{prefix}leaderboard`, `/leaderboard` – Money or XP rankings."
                    ],
                    "explain": (
                        "Secure earnings in the bank with deposit. Better bank plans increase hourly interest. "
                        "Enable autodeposit after voting to automatically secure passive income."
                    )
                },
                "assets": {
                    "title": "🏗️ Assets & Empire",
                    "commands": [
                        f"`{prefix}shop`, `/shop` – Browse passive income assets.",
                        f"`{prefix}buy <id>`, `/buy` – Buy assets.",
                        f"`{prefix}inventory`, `/inventory` – View your assets.",
                        f"`{prefix}profile`, `/profile` – Empire overview with Titles & Medals.",
                        f"`{prefix}prestige`, `/prestige` – Reset for permanent multipliers.",
                        f"`{prefix}buyrole`, `/buyrole` – Buy server roles with coins.",
                        f"`{prefix}alliance create|join|info`, `/alliance` – Alliance management.",
                        f"`{prefix}vassal sponsor @user [percent]`, `/vassal` – Sponsor vassals.",
                        f"`{prefix}market list|view|buy`, `/market` – Player marketplace."
                    ],
                    "explain": (
                        "Invest coins into assets that pay every 10 minutes. Prestige resets progress for permanent multipliers."
                    )
                },
                "wonder": {
                    "title": "🏛️ Wonder & Server Progress",
                    "commands": [
                        f"`{prefix}wonder`, `/wonder` – View Wonder status.",
                        f"`{prefix}contribute <amount>`, `/contribute` – Fund the Wonder."
                    ],
                    "explain": (
                        "Coordinate contributions to level the Wonder and unlock powerful server‑wide boosts."
                    )
                },
                "boosters": {
                    "title": "🚀 Boosters & Rewards",
                    "commands": [
                        f"`{prefix}vote`, `/vote` – Vote for rewards & auto‑deposit.",
                        "**Join Support Server** – 2x Coin Multiplier.",
                        "**Global Leaderboards** – Top ranks grant multipliers and titles."
                    ],
                    "explain": (
                        "Boost earnings via voting, support server bonuses, and leaderboard rewards."
                    )
                },
                "moderation": {
                    "title": "🛡️ Moderation",
                    "commands": [
                        f"`{prefix}kick`, `/kick` – Kick a member.",
                        f"`{prefix}ban`, `/ban` – Ban a member.",
                        f"`{prefix}warn`, `/warn` – Issue a warning.",
                        f"`{prefix}clearwarnings`, `/clearwarnings` – Clear all warns.",
                        f"`{prefix}delwarn`, `/delwarn` – Delete a warn by ID.",
                        f"`{prefix}removewarn`, `/removewarn` – Alias for delwarn.",
                        f"`{prefix}automod add/remove`, `/automod` – Word filter management.",
                        f"`{prefix}setlogs`, `/setlogs` – Configure log channels.",
                        f"`{prefix}raidmode on|off`, `/raidmode` – Lock down during raids.",
                        f"`{prefix}antiphish on|off`, `/antiphish` – Scam link guard.",
                        f"`{prefix}modsystem`, `/modsystem` – Create mod roles & tracking.",
                        f"`{prefix}mod profile`, `/mod profile` – View your mod points.",
                        f"`{prefix}mods`, `/mods` – List tracked moderators.",
                        f"`{prefix}mod lb`, `/mod lb` – Mod leaderboard."
                    ],
                    "explain": (
                        "Configure automod and use kick/ban/warns to keep the server safe. "
                        "Set log channels to record actions in dedicated channels."
                    )
                },
                "info": {
                    "title": "🧭 Utility & Information",
                    "commands": [
                        f"`{prefix}ping`, `/ping` – Bot latency.",
                        f"`{prefix}serverinfo`, `/serverinfo` – Server stats.",
                        f"`{prefix}userinfo`, `/userinfo` – User stats.",
                        f"`{prefix}avatar`, `/avatar` – User avatar.",
                        f"`{prefix}membercount`, `/membercount` – Member stats.",
                        f"`{prefix}leaderboard`, `/leaderboard` – Rankings."
                    ],
                    "explain": (
                        "Quickly inspect server and user information."
                    )
                },
                "welcome": {
                    "title": "📑 Welcome & Configuration",
                    "commands": [
                        f"`{prefix}set welcome` – Configure welcome messages.",
                        f"`{prefix}set farewell` – Configure farewell messages.",
                        f"`{prefix}setlogs`, `/setlogs` – Logging channels.",
                        f"`{prefix}setprefix`, `/setprefix` – Change prefix.",
                        f"`{prefix}setup`, `/setup` – Dashboard link."
                    ],
                    "explain": (
                        "Customize join/leave messages, logging, and prefix. Access the dashboard for advanced config."
                    )
                },
                "owner": {
                    "title": "👑 Owner & Admin",
                    "commands": [
                        f"`{prefix}addmoney`, `/addmoney` – Grant coins.",
                        f"`{prefix}addxp`, `/addxp` – Grant XP.",
                        f"`{prefix}addtitle`, `/addtitle` – Grant a title."
                    ],
                    "explain": (
                        "Restricted commands for bot owners and administrators."
                    )
                },
                "utility": {
                    "title": "⚙️ Setup & Utility",
                    "commands": [
                        f"`{prefix}help`, `/help` – Overview and category help.",
                        f"`{prefix}rank`, `/rank` – Level & XP bar.",
                        f"`{prefix}setup`, `/setup` – Dashboard link.",
                        f"`{prefix}setprefix`, `/setprefix` – Change prefix.",
                        f"`{prefix}start`, `/start` – Tutorial.",
                        f"`{prefix}bounty @user <amount>`, `/bounty` – Place a bounty.",
                        f"`{prefix}remind <10m|2h|1d> [text]`, `/remind` – DM reminders.",
                        f"`{prefix}poll <question> options:\"A,B,C\"`, `/poll` – Multi‑choice poll."
                    ],
                    "explain": (
                        "Use start to onboard new players and help to explore features."
                    )
                }
            }

            data = categories[key]
            embed = discord.Embed(
                title=f"{data['title']}",
                description=data["explain"],
                color=0x00d2ff
            )
            cmds_text = "\n".join(f"- {line}" for line in data["commands"])
            embed.add_field(name="Commands", value=cmds_text, inline=False)
            embed.set_footer(text=f"Requested by {interaction.user.display_name}", icon_url=interaction.user.display_avatar.url)
            await interaction.response.edit_message(embed=embed)
        except Exception as e:
            try:
                await interaction.response.send_message("❌ Failed to update help. Try again.", ephemeral=True)
            except:
                await interaction.followup.send("❌ Failed to update help. Try again.", ephemeral=True)
            print(f"HelpSelect error: {e}")

async def resolve_channel(guild, raw_id):
    try:
        cid = int(raw_id)
    except Exception:
        cid = raw_id
    ch = guild.get_channel(cid)
    if ch is None:
        try:
            ch = await guild.fetch_channel(cid)
        except:
            return None
    return ch

def _resolve_text_mentions(guild: discord.Guild, text: str) -> str:
    if not text:
        return text
    try:
        import re
        def repl_channel(m):
            name = m.group(1)
            for ch in guild.channels:
                if getattr(ch, "type", None) == discord.ChannelType.text and ch.name == name:
                    return f"<#{ch.id}>"
            return f"#{name}"
        def repl_emoji(m):
            name = m.group(1)
            for e in guild.emojis:
                if e.name == name:
                    return f"<:{e.name}:{e.id}>"
            return f":{name}:"
        # Allow optional spaces after '#', e.g. '# rules'
        text = re.sub(r"(?<!\\w)#\\s*([A-Za-z0-9_\\-]+)", repl_channel, text)
        text = re.sub(r":([A-Za-z0-9_\\-]+):", repl_emoji, text)
    except:
        pass
    return text

async def _cfg_get(guild_id: int, keys: list[str]) -> dict:
    out = {}
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            cols = ", ".join(keys)
            async with db.execute(f'SELECT {cols} FROM guild_config WHERE guild_id = ?', (guild_id,)) as c:
                row = await c.fetchone()
        if row:
            for i, k in enumerate(keys):
                out[k] = row[i]
    except:
        pass
    return out
def _apply_placeholders_member(guild: discord.Guild, member: discord.Member) -> dict:
    return {
        "{user}": member.mention,
        "{username}": member.name,
        "{server}": guild.name,
        "{member_count}": str(guild.member_count),
        "{avatar}": member.display_avatar.url,
        "{join_date}": member.joined_at.strftime("%b %d, %Y") if member.joined_at else ""
    }

def _replace_in_data(guild: discord.Guild, data, placeholders: dict):
    if isinstance(data, str):
        s = data
        for k, v in placeholders.items():
            s = s.replace(k, v)
        s = _resolve_text_mentions(guild, s)
        return s
    if isinstance(data, dict):
        return {k: _replace_in_data(guild, v, placeholders) for k, v in data.items()}
    if isinstance(data, list):
        return [_replace_in_data(guild, i, placeholders) for i in data]
    return data

class HelpView(discord.ui.View):
    def __init__(self, prefix, author_id, show_owner):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.add_item(HelpSelect(prefix, show_owner))
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

@bot.hybrid_command(name="prestige", description="Reset your balance and level for a permanent income multiplier")
async def prestige(ctx: commands.Context):
    data = await get_user_data(ctx.author.id, ctx.guild.id)
    
    # Requirement: Level 10 + 50,000 coins in bank
    needed_level = 10
    needed_bank = 50000
    
    if data['level'] < needed_level or data['bank'] < needed_bank:
        return await ctx.send(f"❌ You aren't ready to prestige! You need **Level {needed_level}** and **{needed_bank:,} coins** in your bank.")
    
    embed = discord.Embed(title="✨ Ascend to Greatness?", description=f"Prestiging will reset your **Level, XP, Balance, and Bank** to zero.\n\n**In return, you get:**\n💎 Prestige Level {data['prestige'] + 1}\n🚀 Permanent **{(data['prestige'] + 1) * 50}%** income bonus\n\nType `confirm` to proceed.", color=0xffd700)
    await ctx.send(embed=apply_theme(embed))

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() == 'confirm'

    try:
        await bot.wait_for('message', check=check, timeout=30)
    except:
        return await ctx.send("Prestige cancelled.")

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('UPDATE users SET balance = 100, bank = 0, xp = 0, level = 1, prestige = prestige + 1 WHERE user_id = ? AND guild_id = ?', 
                        (ctx.author.id, ctx.guild.id))
        # Clear assets too? Usually prestige resets everything
        await db.execute('DELETE FROM user_assets WHERE user_id = ? AND guild_id = ?', (ctx.author.id, ctx.guild.id))
        await db.commit()
    
    await ctx.send(f"🎊 **CONGRATULATIONS!** You have reached Prestige Level **{data['prestige'] + 1}**! Your empire begins anew, but stronger than ever.")

@bot.hybrid_command(name="inventory", description="View your owned assets")
async def inventory(ctx: commands.Context, member: discord.Member = None):
    target = member or ctx.author
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT asset_id, count FROM user_assets WHERE user_id = ? AND guild_id = ? AND count > 0', (target.id, ctx.guild.id)) as cursor:
            assets_rows = await cursor.fetchall()
    
    if not assets_rows:
        return await ctx.send(f"📦 {target.display_name} doesn't own any assets yet.")

    assets_config = await get_guild_assets(ctx.guild.id)
    inv_str = ""
    total_income = 0
    
    for aid, count in assets_rows:
        if aid in assets_config:
            name = assets_config[aid]['name']
            income = assets_config[aid]['income'] * count
            inv_str += f"• **{count}x {name}** (Income: 💸 {income:,}/10min)\n"
            total_income += income
        else:
            inv_str += f"• **{count}x {aid}** (Unknown Asset)\n"
            
    embed = discord.Embed(title=f"🎒 {target.display_name}'s Assets", color=0x00d2ff)
    embed.description = inv_str
    embed.add_field(name="📈 Total Passive Income", value=f"💸 {total_income:,} coins / 10 minutes")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="wonder", description="View your server Wonder progress")
async def wonder(ctx: commands.Context):
    data = await get_wonder(ctx.guild.id)
    now = int(time.time())
    goal = data['goal'] or 0
    progress = data['progress']
    level = data['level']
    boost_multiplier = data['boost_multiplier']
    boost_until = data['boost_until']
    progress_pct = int((progress / goal) * 100) if goal > 0 else 0
    bar_length = 12
    filled = int((progress_pct / 100) * bar_length)
    bar = "🟦" * filled + "⬛" * (bar_length - filled)
    if boost_until > now:
        remaining = boost_until - now
        hours, remainder = divmod(remaining, 3600)
        minutes, _ = divmod(remainder, 60)
        boost_status = f"Active • {boost_multiplier:.2f}x • {hours}h {minutes}m left"
    else:
        boost_status = "Inactive"
    embed = discord.Embed(title=f"🏛️ {ctx.guild.name} Wonder", color=0x00d2ff)
    embed.add_field(name="Level", value=f"{level}", inline=True)
    embed.add_field(name="Progress", value=f"{progress:,} / {goal:,} coins", inline=True)
    embed.add_field(name="Boost", value=boost_status, inline=False)
    embed.add_field(name="Progress Bar", value=bar, inline=False)
    embed.set_footer(text="Contribute with /contribute <amount>")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="contribute", description="Contribute coins to your server Wonder")
async def contribute(ctx: commands.Context, amount: int):
    if amount <= 0:
        return await ctx.send("❌ Enter a positive amount.")
    user = await get_user_data(ctx.author.id, ctx.guild.id)
    if user['balance'] < amount:
        return await ctx.send(f"❌ You need **{amount - user['balance']:,} more coins**.")
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR IGNORE INTO guild_wonder (guild_id) VALUES (?)', (ctx.guild.id,))
        async with db.execute('SELECT level, progress, goal, boost_multiplier, boost_until FROM guild_wonder WHERE guild_id = ?', (ctx.guild.id,)) as cursor:
            row = await cursor.fetchone()
        level, progress, goal, boost_multiplier, boost_until = row
        remaining = amount
        leveled_up = 0
        while remaining > 0:
            to_goal = max(0, goal - progress)
            if to_goal == 0:
                level += 1
                goal = int(goal * 1.5 + 10000)
                boost_multiplier = compute_boost_multiplier(level)
                boost_until = now + 21600
                leveled_up += 1
                progress = 0
                continue
            if remaining < to_goal:
                progress += remaining
                remaining = 0
            else:
                remaining -= to_goal
                level += 1
                progress = 0
                goal = int(goal * 1.5 + 10000)
                boost_multiplier = compute_boost_multiplier(level)
                boost_until = now + 21600
                leveled_up += 1
        await db.execute('UPDATE users SET balance = balance - ? WHERE user_id = ? AND guild_id = ?', (amount, ctx.author.id, ctx.guild.id))
        await db.execute('UPDATE guild_wonder SET level = ?, progress = ?, goal = ?, boost_multiplier = ?, boost_until = ? WHERE guild_id = ?', (level, progress, goal, boost_multiplier, boost_until, ctx.guild.id))
        await db.commit()
    if leveled_up > 0:
        await ctx.send(f"🏛️ **Wonder Level Up!** Your server reached **Level {level}** and unlocked **{boost_multiplier:.2f}x** passive income for 6 hours.")
    else:
        await ctx.send(f"✅ Contributed **{amount:,} coins** to the Wonder. Progress: **{progress:,} / {goal:,}**.")

@bot.hybrid_command(name="roulette", description="Bet your coins on a roulette spin")
async def roulette(ctx: commands.Context, amount: str = None, space: str = None):
    if amount is None or space is None:
        prefix = await get_prefix(bot, ctx.message)
        return await ctx.send(f"❌ Incorrect format! Use: `{prefix}roulette <amount> <space>`")
    
    user = await get_user_data(ctx.author.id, ctx.guild.id)
    balance = user['balance']

    if amount.lower() == 'all':
        bet_amount = balance
    elif amount.lower() == 'half':
        bet_amount = balance // 2
    else:
        try:
            bet_amount = int(amount)
        except ValueError:
            return await ctx.send("❌ Invalid amount! Use a number, 'half', or 'all'.")

    if bet_amount <= 0: return await ctx.send("❌ Bet a positive amount!")
    if balance < bet_amount: return await ctx.send("❌ You don't have enough coins!")

    space = space.lower()
    
    # Define valid spaces and their multipliers
    # red/black = 2x, 1st/2nd/3rd = 3x, green = 14x, number = 36x
    valid_colors = ['red', 'black', 'green']
    valid_dozens = ['1st', '2nd', '3rd']
    
    is_number = False
    try:
        num = int(space)
        if 0 <= num <= 36:
            is_number = True
        else:
            return await ctx.send("❌ Number must be between 0 and 36!")
    except ValueError:
        if space not in valid_colors and space not in valid_dozens:
            return await ctx.send("❌ Invalid space! Use: `red`, `black`, `green`, `1st`, `2nd`, `3rd`, or a number `0-36`.")
    
    # Roll logic
    roll = random.randint(0, 36)
    
    # Determine roll color
    if roll == 0: 
        roll_color = 'green'
    elif roll in [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]:
        roll_color = 'red'
    else:
        roll_color = 'black'
        
    # Determine roll dozen
    if 1 <= roll <= 12: roll_dozen = '1st'
    elif 13 <= roll <= 24: roll_dozen = '2nd'
    elif 25 <= roll <= 36: roll_dozen = '3rd'
    else: roll_dozen = None

    # Check win
    win = False
    multiplier = 0
    
    if is_number:
        if int(space) == roll:
            win = True
            multiplier = 36
    elif space == roll_color:
        win = True
        multiplier = 14 if space == 'green' else 2
    elif space == roll_dozen:
        win = True
        multiplier = 3

    async with aiosqlite.connect(DB_FILE) as db:
        if win:
            server_multiplier = get_server_join_multiplier(ctx.author.id)
            winnings = int(bet_amount * (multiplier - 1) * server_multiplier)
            await db.execute('UPDATE users SET balance = balance + ? WHERE user_id = ? AND guild_id = ?', (winnings, ctx.author.id, ctx.guild.id))
            
            boost_msg = ""
            if server_multiplier > 1.0:
                boost_msg = " (Includes **2x Server Booster**!)"
                
            result_msg = f"✅ **WIN!** The ball landed on **{roll_color.upper()} {roll}**.\nYou won **{winnings:,} coins**!{boost_msg}"
            color_embed = 0x2ecc71 # Green
        else:
            await db.execute('UPDATE users SET balance = balance - ? WHERE user_id = ? AND guild_id = ?', (bet_amount, ctx.author.id, ctx.guild.id))
            result_msg = f"❌ **LOSS!** The ball landed on **{roll_color.upper()} {roll}**.\nYou lost **{bet_amount:,} coins**."
            color_embed = 0xe74c3c # Red
        await db.commit()
    
    embed = discord.Embed(title="🎡 Roulette Spin", description=result_msg, color=color_embed)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="riddle", description="Get a riddle to solve")
async def riddle(ctx: commands.Context):
    riddles = [
        ("What has to be broken before you can use it?", "egg"),
        ("I’m tall when I’m young, and I’m short when I’m old. What am I?", "candle"),
        ("What is full of holes but still holds water?", "sponge"),
        ("What gets wet while drying?", "towel"),
        ("What has a head and a tail but no body?", "coin"),
        ("What has keys but can't open locks?", "piano"),
        ("The more of this there is, the less you see. What is it?", "darkness")
    ]
    q, a = random.choice(riddles)
    
    # Store the active riddle in a temporary dictionary
    if not hasattr(bot, 'active_riddles'):
        bot.active_riddles = {}
    
    bot.active_riddles[ctx.author.id] = {
        'answer': a,
        'reward': random.randint(400, 800),
        'expires': time.time() + 60
    }
    
    embed = discord.Embed(title="🧩 Riddle Challenge", description=f"*{q}*", color=0xf1c40f)
    prefix = await get_prefix(bot, ctx.message)
    embed.set_footer(text=f"Use {prefix}answer <your answer> to solve! (60s)")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="answer", description="Answer an active riddle")
async def answer(ctx: commands.Context, *, response: str):
    if not hasattr(bot, 'active_riddles') or ctx.author.id not in bot.active_riddles:
        return await ctx.send("❌ You don't have an active riddle! Use `.riddle` first.")
    
    riddle_data = bot.active_riddles[ctx.author.id]
    
    if time.time() > riddle_data['expires']:
        del bot.active_riddles[ctx.author.id]
        return await ctx.send("⏰ Your riddle has expired! Try again with `.riddle`.")
    
    if response.lower().strip() == riddle_data['answer']:
        reward = riddle_data['reward']
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('UPDATE users SET balance = balance + ? WHERE user_id = ? AND guild_id = ?', 
                            (reward, ctx.author.id, ctx.guild.id))
            await db.commit()
        
        # Use helper for XP to trigger level up notifications
        leveled_up, new_level = await add_xp(ctx.author.id, ctx.guild.id, 50)
        
        del bot.active_riddles[ctx.author.id]
        msg = f"✅ **CORRECT!** You earned **{reward:,} coins**!"
        if leveled_up:
            msg += f"\n🎊 **LEVEL UP!** You reached **Level {new_level}**!"
        await ctx.send(msg)
    else:
        # Don't delete on wrong answer, let them try until timeout
        await ctx.send("❌ That's not it! Try again.")

@bot.hybrid_command(name="blackjack", aliases=["bj"], description="Play a game of Blackjack")
@app_commands.describe(amount="The amount of coins to bet")
async def blackjack(ctx: commands.Context, amount: str = None):
    if amount is None:
        prefix = await get_prefix(bot, ctx.message)
        return await ctx.send(f"❌ Incorrect format! Use: `{prefix}bj <amount>`")
    
    user = await get_user_data(ctx.author.id, ctx.guild.id)
    balance = user['balance']
    job_id = await get_user_job(ctx.author.id, ctx.guild.id)
    # The multiplier logic is now integrated into win calculation
    # bj_multiplier = 1.0 # Removed unused variable

    if amount.lower() == 'all':
        bet_amount = balance
    elif amount.lower() == 'half':
        bet_amount = balance // 2
    else:
        try:
            bet_amount = int(amount)
        except ValueError:
            return await ctx.send("❌ Invalid amount! Use a number, 'half', or 'all'.")

    if bet_amount <= 0: return await ctx.send("❌ Bet a positive amount!")
    if balance < bet_amount: return await ctx.send("❌ You don't have enough coins!")

    # Deck setup
    suits = {'♠': '♠️', '♥': '♥️', '♦': '♦️', '♣': '♣️'}
    values = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
    
    def get_card():
        val = random.choice(values)
        suit_icon = random.choice(list(suits.values()))
        return val, suit_icon

    def calc_hand(hand):
        total = 0
        aces = 0
        for val, _ in hand:
            if val in ['J', 'Q', 'K']: total += 10
            elif val == 'A': aces += 1
            else: total += int(val)
        for _ in range(aces):
            if total + 11 <= 21: total += 11
            else: total += 1
        return total

    player_hand = [get_card(), get_card()]
    dealer_hand = [get_card(), get_card()]

    def format_hand(hand, hide_first=False):
        if hide_first:
            # Show the back emoji for the first card, and the emoji for the second card
            back_emoji = CARD_EMOJIS.get('back', '🎴')
            second_card = hand[1]
            second_emoji = CARD_EMOJIS.get((second_card[0], second_card[1]), f"**[{second_card[0]}]** {second_card[1]}")
            return f"{back_emoji} {second_emoji}"
        
        emojis = []
        for val, suit in hand:
            emoji = CARD_EMOJIS.get((val, suit))
            if emoji:
                emojis.append(emoji)
            else:
                # Fallback for missing cards (J, Q, K, 10 of Spades, Ace of Spades)
                emojis.append(f"**[{val}]** {suit}")
        
        return " ".join(emojis)

    # Determine split availability
    can_split = player_hand[0][0] == player_hand[1][0]
    class BlackjackView(discord.ui.View):
        def __init__(self, ctx, can_double=True, can_split=False):
            super().__init__(timeout=30)
            self.ctx = ctx
            self.value = None
            if not can_double:
                self.double_down.disabled = True
            if not can_split:
                self.split.disabled = True

        @discord.ui.button(label="Hit", style=discord.ButtonStyle.primary, custom_id="hit")
        async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
            if interaction.user.id != self.ctx.author.id:
                return await interaction.response.send_message("This isn't your game!", ephemeral=True)
            self.value = "hit"
            await interaction.response.defer()
            self.stop()

        @discord.ui.button(label="Stand", style=discord.ButtonStyle.secondary, custom_id="stand")
        async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
            if interaction.user.id != self.ctx.author.id:
                return await interaction.response.send_message("This isn't your game!", ephemeral=True)
            self.value = "stand"
            await interaction.response.defer()
            self.stop()

        @discord.ui.button(label="Double Down", style=discord.ButtonStyle.secondary, custom_id="double")
        async def double_down(self, interaction: discord.Interaction, button: discord.ui.Button):
            if interaction.user.id != self.ctx.author.id:
                return await interaction.response.send_message("This isn't your game!", ephemeral=True)
            self.value = "double"
            await interaction.response.defer()
            self.stop()

        @discord.ui.button(label="Split", style=discord.ButtonStyle.secondary, custom_id="split")
        async def split(self, interaction: discord.Interaction, button: discord.ui.Button):
            if interaction.user.id != self.ctx.author.id:
                return await interaction.response.send_message("This isn't your game!", ephemeral=True)
            if not can_split:
                return await interaction.response.send_message("You can only split identical ranks.", ephemeral=True)
            self.value = "split"
            await interaction.response.defer()
            self.stop()

    async def get_bj_embed(show_dealer=False, result_text=None):
        # Using a bright color as requested (Cyan/Bright Blue)
        embed = discord.Embed(color=0x00FFFF) 
        embed.set_author(name=f"{ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
        
        # Dealer side
        if show_dealer:
            d_val = calc_hand(dealer_hand)
            d_str = format_hand(dealer_hand)
        else:
            visible_card_value = calc_hand([dealer_hand[1]])
            d_val = visible_card_value
            d_str = format_hand(dealer_hand, hide_first=True)
        
        # Player side
        p_val = calc_hand(player_hand)

        if result_text:
            # Result formatting like UnbelievaBoat
            embed.description = f"**Result: {result_text}**"
            if "Win" in result_text: embed.color = 0x00ff00 # Bright Green
            elif "Loss" in result_text or "Bust" in result_text or "Timed Out" in result_text: embed.color = 0xff0000 # Bright Red
            else: embed.color = 0xffff00 # Bright Yellow
        
        # Hand display side-by-side
        embed.add_field(name="Your Hand", value=f"{format_hand(player_hand)}\n\n**Value: {p_val}**", inline=True)
        embed.add_field(name="Dealer Hand", value=f"{d_str}\n\n**Value: {d_val}**", inline=True)
        
        return embed

    view = BlackjackView(ctx, can_double=(balance >= bet_amount * 2), can_split=can_split)
    msg = await ctx.send(embed=await get_bj_embed(), view=view)

    # Game Loop
    while True:
        if calc_hand(player_hand) >= 21:
            break
            
        await view.wait()
        
        if view.value == "hit":
            player_hand.append(get_card())
            if calc_hand(player_hand) >= 21:
                break
            view = BlackjackView(ctx, can_double=False) # Can't double after hitting
            await msg.edit(embed=await get_bj_embed(), view=view)
        elif view.value == "stand":
            break
        elif view.value == "double":
            bet_amount *= 2
            player_hand.append(get_card())
            break
        elif view.value == "split":
            hand1 = [player_hand[0], get_card()]
            hand2 = [player_hand[1], get_card()]
            # Simple auto-play strategy for split: hit until 17+
            while calc_hand(hand1) < 17:
                hand1.append(get_card())
            while calc_hand(hand2) < 17:
                hand2.append(get_card())
            # Dealer plays
            while calc_hand(dealer_hand) < 17:
                dealer_hand.append(get_card())
            # Evaluate both hands
            results = []
            for h in [hand1, hand2]:
                p_total = calc_hand(h)
                d_total = calc_hand(dealer_hand)
                if p_total > 21:
                    results.append("loss")
                elif d_total > 21 or p_total > d_total:
                    results.append("win")
                elif p_total == d_total:
                    results.append("push")
                else:
                    results.append("loss")
            # Apply settlements: each hand is one bet
            total_delta = 0
            if "win" in results: total_delta += bet_amount
            if results.count("win") == 2: total_delta += bet_amount
            if results.count("loss") == 1: total_delta -= bet_amount
            if results.count("loss") == 2: total_delta -= bet_amount * 2
            async with aiosqlite.connect(DB_FILE) as db:
                if total_delta > 0:
                    await db.execute('UPDATE users SET balance = balance + ?, blackjack_wins = blackjack_wins + ? WHERE user_id = ? AND guild_id = 0', (total_delta, results.count("win"), ctx.author.id))
                elif total_delta < 0:
                    await db.execute('UPDATE users SET balance = balance - ? WHERE user_id = ? AND guild_id = 0', (abs(total_delta), ctx.author.id))
                await db.commit()
            await increment_stat(ctx.author.id, ctx.guild.id, "blackjack_plays")
            if results.count("win") > 0:
                await increment_stat(ctx.author.id, ctx.guild.id, "blackjack_wins")
            summary = f"Split result — Wins: {results.count('win')}, Pushes: {results.count('push')}, Losses: {results.count('loss')}."
            await msg.edit(embed=await get_bj_embed(show_dealer=True, result_text=summary), view=None)
            return

    # Dealer Turn
    p_total = calc_hand(player_hand)
    if p_total > 21:
        result = f"Bust 🍞 -{bet_amount:,}"
        win_status = "loss"
    else:
        # Dealer must hit until 17
        while calc_hand(dealer_hand) < 17:
            dealer_hand.append(get_card())
        
        d_total = calc_hand(dealer_hand)
        if d_total > 21:
            result = f"Win 🍞 +{bet_amount:,}"
            win_status = "win"
        elif d_total > p_total:
            result = f"Loss 🍞 -{bet_amount:,}"
            win_status = "loss"
        elif d_total < p_total:
            result = f"Win 🍞 +{bet_amount:,}"
            win_status = "win"
        else:
            result = f"Push 🍞 +0"
            win_status = "push"

    async with aiosqlite.connect(DB_FILE) as db:
        if win_status == "win":
            server_multiplier = get_server_join_multiplier(ctx.author.id)
            final_win = int(bet_amount * server_multiplier)
            await db.execute('UPDATE users SET balance = balance + ?, blackjack_wins = blackjack_wins + 1 WHERE user_id = ? AND guild_id = 0', (final_win, ctx.author.id))
            if server_multiplier > 1.0:
                result += f" ({server_multiplier}x Server Boost!)"
        elif win_status == "loss":
            await db.execute('UPDATE users SET balance = balance - ? WHERE user_id = ? AND guild_id = 0', (bet_amount, ctx.author.id))
        await db.commit()
    await increment_stat(ctx.author.id, ctx.guild.id, "blackjack_wins" if win_status == "win" else "blackjack_plays")

    await msg.edit(embed=await get_bj_embed(show_dealer=True, result_text=result), view=None)

 

 

@bot.hybrid_command(name="vote", description="Vote for the bot on Top.gg to get rewards!")
async def vote(ctx: commands.Context):
    await ctx.defer()
    vote_url = f"https://top.gg/bot/{bot.user.id}/vote"
    data = await get_user_data(ctx.author.id, ctx.guild.id)
    now = int(time.time())
    last_vote_time = data['last_vote'] if data['last_vote'] else 0
    time_since_vote = now - last_vote_time
    
    # Debug logging
    print(f"DEBUG: /vote command - User: {ctx.author.id}, Last Vote: {last_vote_time}, Now: {now}, Time Since: {time_since_vote}s")
    
    embed = discord.Embed(title="🗳️ Vote for Empire Nexus", color=0x00d2ff)
    embed.description = f"Support the bot and unlock exclusive rewards for **12 hours**!\n\n" \
                        f"🎁 **Rewards:**\n" \
                        f"• 🏦 **Auto-Deposit:** Passive income goes straight to your bank!\n" \
                        f"• 💰 **Bonus Coins:** 25,000 Coins (Instant)\n\n" \
                        f"[**Click here to vote on Top.gg**]({vote_url})"
    
    if time_since_vote < 43200:
        remaining = 43200 - time_since_vote
        hours, remainder = divmod(remaining, 3600)
        minutes, _ = divmod(remainder, 60)
        embed.add_field(name="✅ Status", value=f"You have already voted! Rewards active for **{hours}h {minutes}m**.")
    else:
        embed.add_field(name="❌ Status", value="You haven't voted in the last 12 hours.")
        
    await ctx.send(embed=embed)

@bot.hybrid_command(name="autodeposit", description="Toggle auto-deposit of passive income (requires active vote)")
async def autodeposit(ctx: commands.Context):
    await ctx.defer()
    data = await get_user_data(ctx.author.id, ctx.guild.id)
    now = int(time.time())
    last_vote_time = data['last_vote'] if data['last_vote'] else 0
    time_since_vote = now - last_vote_time
    is_voter = time_since_vote < 43200
    
    # Debug logging
    print(f"DEBUG: /autodeposit command - User: {ctx.author.id}, Last Vote: {last_vote_time}, Now: {now}, Time Since: {time_since_vote}s, Is Voter: {is_voter}")
    
    if not is_voter:
        vote_url = f"https://top.gg/bot/{bot.user.id}/vote"
        return await ctx.send(f"❌ You need an active vote to use this! [**Vote here**]({vote_url}) to unlock auto-deposit for 12 hours.")
    
    new_state = 0 if data['auto_deposit'] else 1
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('UPDATE users SET auto_deposit = ? WHERE user_id = ? AND guild_id = ?', (new_state, ctx.author.id, ctx.guild.id))
        await db.commit()
    
    if new_state:
        remaining = 43200 - time_since_vote
        hours, remainder = divmod(remaining, 3600)
        minutes, _ = divmod(remainder, 60)
        await ctx.send(f"✅ **Auto-deposit starting now!** You have **{hours}h {minutes}m** left until your vote expires.")
    else:
        await ctx.send("✅ Auto-deposit is now **DISABLED**.")

 

@bot.hybrid_command(name="profile", description="View your empire status")
async def profile(ctx: commands.Context, member: discord.Member = None):
    target = member or ctx.author
    data = await get_global_money(target.id)
    await ensure_rewards(target.id)
    
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT asset_id, count FROM user_assets WHERE user_id = ? AND guild_id = ? AND count > 0', (target.id, ctx.guild.id)) as cursor:
            assets_rows = await cursor.fetchall()
        async with db.execute('SELECT multipliers_json, titles_json, medals_json FROM user_rewards WHERE user_id = ?', (target.id,)) as cursor:
            reward_row = await cursor.fetchone()
    
    assets_str = "\n".join([f"• {count}x {aid}" for aid, count in assets_rows]) if assets_rows else "No assets."
    
    titles_str = "None"
    medals_str = ""
    if reward_row:
        try:
            titles = json.loads(reward_row['titles_json'])
            medals = json.loads(reward_row['medals_json'])
            if titles:
                titles_str = ", ".join([t['title'] for t in titles])
            if medals:
                medals_str = " " + " ".join([m['medal'] for m in medals])
        except:
            pass
    
    embed = discord.Embed(title=f"👑 {target.display_name}'s Empire{medals_str}", color=0x00d2ff)
    embed.add_field(name="📊 Stats", value=f"Level: {data['level']}\nXP: {data['xp']}\nPrestige: {data['prestige']}", inline=True)
    embed.add_field(name="💰 Wealth (Global)", value=f"Wallet: {data['balance']:,}\nBank: {data['bank']:,}", inline=True)
    embed.add_field(name="🏷️ Titles", value=titles_str, inline=False)
    embed.add_field(name="🏗️ Assets", value=assets_str, inline=False)
    await ctx.send(embed=embed)

 

 

# --- Hybrid Commands (Prefix + Slash) ---

 

 

 

 

@bot.hybrid_command(name="buyrole", description="Buy a role from the server shop")
async def buyrole(ctx: commands.Context, role: discord.Role):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT role_shop_json FROM guild_config WHERE guild_id = ?', (ctx.guild.id,)) as cursor:
            row = await cursor.fetchone()
            if not row: return await ctx.send("❌ This server hasn't set up a role shop yet!")
            shop = json.loads(row[0])

    role_id = str(role.id)
    if role_id not in shop:
        return await ctx.send("❌ This role is not for sale!")

    price = shop[role_id]
    user = await get_global_money(ctx.author.id)

    if user['balance'] < price:
        return await ctx.send(f"❌ You need **{price - user['balance']:,} more coins**!")

    try:
        await ctx.author.add_roles(role)
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('UPDATE users SET balance = balance - ? WHERE user_id = ? AND guild_id = 0', (price, ctx.author.id))
            await db.commit()
        await ctx.send(f"✅ Successfully bought the **{role.name}** role!")
    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to give you that role! (Make sure my role is higher than the one you're buying)")

 

 

@bot.hybrid_command(name="marry", description="Marry another user")
async def marry(ctx: commands.Context, member: discord.Member):
    if member.id == ctx.author.id:
        return await ctx.send("You can't marry yourself.")
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT partner_id FROM marriages WHERE user_id = ?', (ctx.author.id,)) as c:
            row = await c.fetchone()
        async with db.execute('SELECT partner_id FROM marriages WHERE user_id = ?', (member.id,)) as c2:
            row2 = await c2.fetchone()
        if row or row2:
            return await ctx.send("Either you or the target is already married.")
        await db.execute('INSERT OR REPLACE INTO marriage_proposals (proposer_id, target_id, guild_id, created_at) VALUES (?, ?, ?, ?)', (ctx.author.id, member.id, ctx.guild.id, int(time.time())))
        await db.commit()
    await ctx.send(f"💌 {member.mention}, {ctx.author.mention} proposed! Use `/acceptmarry @{ctx.author.display_name}` or `/declinemarry @{ctx.author.display_name}`.")

@bot.hybrid_command(name="divorce", description="Divorce your current partner")
async def divorce(ctx: commands.Context):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT partner_id FROM marriages WHERE user_id = ?', (ctx.author.id,)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("You're not married.")
        partner_id = row[0]
        async with db.execute('SELECT kids FROM marriages WHERE user_id = ?', (ctx.author.id,)) as kc:
            krow = await kc.fetchone()
        kids = int(krow[0] or 0) if krow else 0
        if kids > 0:
            questions = [
                "Who has more stable availability for childcare?",
                "Who contributes more to family finances?",
                "Who has better support network in the server?",
                "Who has shown more consistency in daily engagement?",
                "Who can provide safer environment (moderation record)?"
            ]
            await db.execute('INSERT INTO divorce_cases (guild_id, spouse1_id, spouse2_id, kids, questions_json) VALUES (?, ?, ?, ?, ?)', (ctx.guild.id, ctx.author.id, partner_id, kids, json.dumps(questions)))
            await db.commit()
            await ctx.send("⚖️ Court case opened. Both spouses must answer via `/divorce_answer case_id:<id> answers:\"A,B,C,D,E\"` where A/B indicates which spouse for each question.")
            return
        await db.execute('DELETE FROM marriages WHERE user_id = ?', (ctx.author.id,))
        await db.execute('DELETE FROM marriages WHERE user_id = ?', (partner_id,))
        await db.commit()
    await ctx.send("💔 Divorce finalized.")

@bot.hybrid_command(name="acceptmarry", description="Accept a marriage proposal")
async def acceptmarry(ctx: commands.Context, member: discord.Member):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT created_at FROM marriage_proposals WHERE proposer_id = ? AND target_id = ? AND guild_id = ?', (member.id, ctx.author.id, ctx.guild.id)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("No proposal found.")
        await db.execute('DELETE FROM marriage_proposals WHERE proposer_id = ? AND target_id = ? AND guild_id = ?', (member.id, ctx.author.id, ctx.guild.id))
        await db.execute('INSERT OR REPLACE INTO marriages (user_id, partner_id, kids) VALUES (?, ?, COALESCE((SELECT kids FROM marriages WHERE user_id = ?), 0))', (ctx.author.id, member.id, ctx.author.id))
        await db.execute('INSERT OR REPLACE INTO marriages (user_id, partner_id, kids) VALUES (?, ?, COALESCE((SELECT kids FROM marriages WHERE user_id = ?), 0))', (member.id, ctx.author.id, member.id))
        await db.commit()
    await ctx.send(f"💍 {ctx.author.mention} and {member.mention} are now married! Congratulations!")

@bot.hybrid_command(name="declinemarry", description="Decline a marriage proposal")
async def declinemarry(ctx: commands.Context, member: discord.Member):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('DELETE FROM marriage_proposals WHERE proposer_id = ? AND target_id = ? AND guild_id = ?', (member.id, ctx.author.id, ctx.guild.id))
        await db.commit()
    await ctx.send("❌ Proposal declined.")

@bot.hybrid_command(name="divorce_answer", description="Answer divorce case questions")
async def divorce_answer(ctx: commands.Context, case_id: int, answers: str):
    parts = [p.strip().upper() for p in answers.split(",") if p.strip()]
    if len(parts) != 5 or any(p not in ["A","B"] for p in parts):
        return await ctx.send("Provide 5 answers as A or B separated by commas.")
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM divorce_cases WHERE case_id = ? AND status = "pending"', (case_id,)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("Case not found or already closed.")
        s1 = int(row['spouse1_id']); s2 = int(row['spouse2_id'])
        ajson = json.dumps(parts)
        if ctx.author.id == s1:
            await db.execute('UPDATE divorce_cases SET answers1_json = ? WHERE case_id = ?', (ajson, case_id))
        elif ctx.author.id == s2:
            await db.execute('UPDATE divorce_cases SET answers2_json = ? WHERE case_id = ?', (ajson, case_id))
        else:
            return await ctx.send("You are not part of this case.")
        await db.commit()
        async with db.execute('SELECT answers1_json, answers2_json, kids FROM divorce_cases WHERE case_id = ?', (case_id,)) as c2:
            row2 = await c2.fetchone()
        if not row2 or not row2['answers1_json'] or not row2['answers2_json']:
            return await ctx.send("Answers recorded. Waiting for the other spouse.")
        a1 = json.loads(row2['answers1_json'])
        a2 = json.loads(row2['answers2_json'])
        score1 = sum(1 for i in range(5) if a1[i] == "A" and a2[i] == "A")
        score2 = sum(1 for i in range(5) if a1[i] == "B" and a2[i] == "B")
        winner = s1 if score1 >= score2 else s2
        loser = s2 if winner == s1 else s1
        kids = int(row2['kids'] or 0)
        base_fine = max(1000, kids * 5000)
        extra_fine = max(0, (score1 - score2) * 1000) if winner == s1 else max(0, (score2 - score1) * 1000)
        fines = {"base": base_fine, "loser_extra": extra_fine}
        await db.execute('UPDATE divorce_cases SET status = "closed", fines_json = ? WHERE case_id = ?', (json.dumps(fines), case_id))
        await db.execute('UPDATE marriages SET kids = ?, partner_id = NULL WHERE user_id = ?', (kids, winner))
        await db.execute('UPDATE marriages SET kids = 0, partner_id = NULL WHERE user_id = ?', (loser))
        await db.commit()
    await update_global_balance(loser, -(base_fine + extra_fine))
    await ctx.send(f"⚖️ Court concluded. Custody awarded to <@{winner}>. Fines: base {base_fine:,}, loser extra {extra_fine:,}.")

@bot.hybrid_command(name="kids", description="Manage or view family kids count")
@app_commands.describe(action="add or view", count="How many to add (if adding)")
async def kids(ctx: commands.Context, action: str = "view", count: int = 0):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT partner_id, kids FROM marriages WHERE user_id = ?', (ctx.author.id,)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("You're not married.")
        partner_id = int(row[0]); current = int(row[1] or 0)
        if action.lower() == "add":
            if count <= 0:
                return await ctx.send("Provide a positive count.")
            newc = current + count
            await db.execute('UPDATE marriages SET kids = ? WHERE user_id = ?', (newc, ctx.author.id))
            await db.execute('UPDATE marriages SET kids = ? WHERE user_id = ?', (newc, partner_id))
            await db.commit()
            return await ctx.send(f"👶 Family updated: kids = {newc}.")
        return await ctx.send(f"👪 Current kids: {current}.")
def win_loss_apply(user_id, amount, win=True):
    delta = amount if win else -amount
    res = update_global_balance(user_id, delta)
    if win and amount > 0:
        bot.loop.create_task(apply_vassal_cut(user_id, 0, amount))
    return res

async def apply_vassal_cut(user_id: int, guild_id: int, amount: int):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT lord_id, percent FROM vassals WHERE vassal_id = ? ORDER BY percent DESC LIMIT 1', (user_id,)) as c:
                row = await c.fetchone()
        if not row:
            return
        lord_id, percent = row
        cut = int(amount * (percent / 100.0))
        if cut > 0:
            await update_global_balance(lord_id, cut)
    except:
        pass

@bot.hybrid_command(name="coinflip", description="50/50 coinflip")
@app_commands.describe(amount="Bet amount or 'all'")
async def coinflip(ctx: commands.Context, amount: str):
    data = await get_global_money(ctx.author.id)
    if amount.lower() == 'all':
        bet = data['balance']
    else:
        try:
            bet = int(amount)
        except:
            return await ctx.send("Enter a valid number or 'all'.")
    if bet <= 0: return await ctx.send("Bet must be positive.")
    if bet > data['balance']: return await ctx.send("You don't have enough coins.")
    win = random.choice([True, False])
    await win_loss_apply(ctx.author.id, bet, win=win)
    await ctx.send("🪙 Heads! You win!" if win else "🪙 Tails! You lose.")

@bot.hybrid_command(name="slots", description="Spin the slot machine")
@app_commands.describe(amount="Bet amount or 'all'")
async def slots(ctx: commands.Context, amount: str):
    data = await get_global_money(ctx.author.id)
    if amount.lower() == 'all':
        bet = data['balance']
    else:
        try:
            bet = int(amount)
        except:
            return await ctx.send("Enter a valid number or 'all'.")
    if bet <= 0: return await ctx.send("Bet must be positive.")
    if bet > data['balance']: return await ctx.send("You don't have enough coins.")
    reels = ['🍒','🍋','🍇','⭐','💎']
    r = [random.choice(reels) for _ in range(3)]
    if r[0] == r[1] == r[2]:
        win_amt = int(bet * 3)
        await win_loss_apply(ctx.author.id, win_amt, win=True)
        await ctx.send(f"🎰 {' '.join(r)} — JACKPOT! +{win_amt:,}")
    elif r[0] == r[1] or r[1] == r[2] or r[0] == r[2]:
        win_amt = int(bet * 1.5)
        await win_loss_apply(ctx.author.id, win_amt, win=True)
        await ctx.send(f"🎰 {' '.join(r)} — Pair! +{win_amt:,}")
    else:
        await win_loss_apply(ctx.author.id, bet, win=False)
        await ctx.send(f"🎰 {' '.join(r)} — No match. -{bet:,}")

@bot.hybrid_command(name="russianroulette", aliases=["rr"], description="Risky game: 1/6 chance to lose")
@app_commands.describe(amount="Bet amount or 'all'")
async def russianroulette(ctx: commands.Context, amount: str):
    data = await get_global_money(ctx.author.id)
    if amount.lower() == 'all':
        bet = data['balance']
    else:
        try:
            bet = int(amount)
        except:
            return await ctx.send("Enter a valid number or 'all'.")
    if bet <= 0: return await ctx.send("Bet must be positive.")
    if bet > data['balance']: return await ctx.send("You don't have enough coins.")
    chamber = random.randint(1,6)
    if chamber == 1:
        await win_loss_apply(ctx.author.id, bet, win=False)
        await ctx.send(f"🔫 Bang! You lost **{bet:,}** coins.")
    else:
        await win_loss_apply(ctx.author.id, bet, win=True)
        await ctx.send(f"🔫 Click! You survived and won **{bet:,}** coins.")

# Leaderboard Cache
LB_CACHE = {}
LB_CACHE_DURATION = 300 # 5 minutes

@bot.hybrid_command(name="leaderboard", aliases=["lb"], description="View the global leaderboard")
@app_commands.choices(category=[
    app_commands.Choice(name="Most Commands Used", value="commands"),
    app_commands.Choice(name="Most Successful Robs", value="robs"),
    app_commands.Choice(name="Most Successful Crimes", value="crimes"),
    app_commands.Choice(name="Most Money", value="money"),
    app_commands.Choice(name="Highest Passive Income", value="passive"),
    app_commands.Choice(name="Highest Level", value="level"),
    app_commands.Choice(name="Blackjack Wins", value="blackjack_wins"),
    app_commands.Choice(name="Highest Wonder Level", value="wonder")
])
@app_commands.choices(scope=[
    app_commands.Choice(name="Global", value="global"),
    app_commands.Choice(name="Server Only", value="server")
])
async def leaderboard(ctx: commands.Context, category: str = "money", scope: str = "global"):
    now = time.time()
    
    # Check cache
    cache_key = f"{scope}:{category}"
    if cache_key in LB_CACHE:
        cache_data, timestamp = LB_CACHE[cache_key]
        if now - timestamp < LB_CACHE_DURATION:
            return await ctx.send(embed=cache_data)

    async with aiosqlite.connect(DB_FILE) as db:
        where = "" if scope == "global" else f" WHERE guild_id = {ctx.guild.id} "
        group = "GROUP BY user_id"
        limit = "LIMIT 10"
        if category == "commands":
            query = f'SELECT user_id, SUM(total_commands) as total FROM users{where} {group} ORDER BY total DESC {limit}'
            title = "🏆 Global Commands Leaderboard"
            symbol = "⌨️"
            unit = "commands"
        elif category == "robs":
            query = f'SELECT user_id, SUM(successful_robs) as total FROM users{where} {group} ORDER BY total DESC {limit}'
            title = "🏆 Global Robbery Leaderboard"
            symbol = "🧤"
            unit = "robs"
        elif category == "crimes":
            query = f'SELECT user_id, SUM(successful_crimes) as total FROM users{where} {group} ORDER BY total DESC {limit}'
            title = "🏆 Global Crime Leaderboard"
            symbol = "😈"
            unit = "crimes"
        elif category == "money":
            query = f'SELECT user_id, SUM(balance + bank) as total FROM users{where} {group} ORDER BY total DESC {limit}'
            title = "🏆 Global Wealth Leaderboard"
            symbol = "🪙"
            unit = "coins"
        elif category == "passive":
            query = f'SELECT user_id, SUM(passive_income) as total FROM users{where} {group} ORDER BY total DESC {limit}'
            title = "🏆 Global Passive Income Leaderboard"
            symbol = "📈"
            unit = "coins/10m"
        elif category == "level":
            query = f'SELECT user_id, MAX(level) as max_level, MAX(xp) as max_xp FROM users{where} {group} ORDER BY max_level DESC, max_xp DESC {limit}'
            title = "🏆 Global Level Leaderboard"
            symbol = "⭐"
            unit = "Level"
        elif category == "blackjack_wins":
            query = f'SELECT user_id, SUM(blackjack_wins) as total FROM users{where} {group} ORDER BY total DESC {limit}'
            title = "🏆 Blackjack Wins Leaderboard"
            symbol = "🃏"
            unit = "wins"
        elif category == "wonder":
            query = f'SELECT guild_id, MAX(level) as total FROM guild_wonder ORDER BY total DESC LIMIT 10'
            title = "🏛️ Highest Wonder Level"
            symbol = "🏛️"
            unit = "Level"

        async with db.execute(query) as cursor:
            rows = await cursor.fetchall()
    
    if not rows: return await ctx.send("The leaderboard is empty!")
    
    lb_str = ""
    for i, row in enumerate(rows, 1):
        uid = row[0]
        val = row[1]
        
        # Medal for top 3
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"**{i}.**"
        
        if category == "wonder":
            gid = uid
            guild = bot.get_guild(gid)
            gname = guild.name if guild else f"Guild({gid})"
            lb_str += f"{medal} **{gname}** — {symbol} Level {val}\n"
        else:
            user = bot.get_user(uid)
            name = user.name if user else f"User({uid})"
            if category == "level":
                max_level = row[1]
                max_xp = row[2]
                lb_str += f"{medal} **{name}** — Lvl {max_level} ({max_xp} XP)\n"
            elif category == "passive":
                lb_str += f"{medal} **{name}** — {symbol} {val:,.2f} {unit}\n"
            else:
                lb_str += f"{medal} **{name}** — {symbol} {val:,} {unit}\n"
    
    lb_str += "\n*Top 3 receive stackable coin multipliers!*"
    
    embed = discord.Embed(title=title, description=lb_str, color=0xFFA500)
    
    # Update cache
    LB_CACHE[cache_key] = (embed, time.time())
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="setup", aliases=["dashboard", "configure"], description="Get the dashboard link to configure the bot")
async def setup_cmd(ctx: commands.Context):
    embed = discord.Embed(
        title="⚙️ Empire Nexus Setup",
        description=(
            "Configure your kingdom, set up the role shop, and create custom assets via the web dashboard.\n\n"
            "🔗 [**Nexus Dashboard**](https://empirenexus.alwaysdata.net/)\n"
            "🛠️ [**Support Server**](https://discord.gg/zsqWFX2gBV)\n\n"
            "*Note: Only server administrators can deploy changes.*"
        ),
        color=0x00d2ff
    )
    embed.set_footer(text="Rule with iron, prosper with gold.")
    await ctx.send(embed=embed)

 

@bot.hybrid_command(name="jobs", description="List available jobs")
async def jobs(ctx: commands.Context):
    current = await get_user_job(ctx.author.id, ctx.guild.id)
    data = await get_user_data(ctx.author.id, ctx.guild.id)
    desc = ""
    for job_id, info in JOBS.items():
        marker = "✅" if job_id == current else "➖"
        name = info.get("name", job_id)
        diff = info.get("difficulty", "Unknown")
        min_level = info.get("min_level", 0)
        mult = float(info.get("multiplier", 1.0))
        desc += f"{marker} **{name}** (`{job_id}`)\nDifficulty: {diff} • Min Lvl: {min_level} • Income x{mult:.2f}\n\n"
    embed = discord.Embed(title="⚒️ Available Jobs", description=desc or "No jobs configured.", color=0x00d2ff)
    embed.set_footer(text=f"Your level: {data['level']}. Use /applyjob <id> to apply.")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="applyjob", description="Apply for a job")
async def applyjob(ctx: commands.Context, job_id: str):
    job_id = job_id.lower()
    if job_id not in JOBS:
        await ctx.send("Invalid job id.")
        return
    info = JOBS[job_id]
    data = await get_user_data(ctx.author.id, ctx.guild.id)
    if await get_user_job(ctx.author.id, ctx.guild.id) == job_id:
        await ctx.send("You already have this job.")
        return
    if data['level'] < info.get("min_level", 0):
        await ctx.send(f"You need at least level {info.get('min_level', 0)} for this job.")
        return
    question = info.get("question", "")
    answer = info.get("answer", "").lower()
    if not question or not answer:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR REPLACE INTO user_jobs (user_id, guild_id, job_id) VALUES (?, ?, ?)', (ctx.author.id, ctx.guild.id, job_id))
            await db.commit()
        await ctx.send(f"You are now hired as **{info.get('name', job_id)}**.")
        return
    await ctx.send(f"Application question for **{info.get('name', job_id)}**:\n{question}")
    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel
    try:
        reply = await bot.wait_for('message', check=check, timeout=60)
    except:
        await ctx.send("Application timed out.")
        return
    
    if reply.content.lower() == answer:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR REPLACE INTO user_jobs (user_id, guild_id, job_id) VALUES (?, ?, ?)', (ctx.author.id, ctx.guild.id, job_id))
            await db.commit()
        await ctx.send(f"✅ Correct! You are now hired as **{info.get('name', job_id)}**.")
    else:
        await ctx.send(f"❌ Incorrect answer. You failed the application for **{info.get('name', job_id)}**.")

# --- Utility Commands ---

@bot.hybrid_command(name="raidmode", description="Toggle Raid Mode for this server")
@owner_or_admin()
async def raidmode(ctx: commands.Context, state: str):
    val = 1 if str(state).lower() in ["on","enable","enabled","true","1"] else 0
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR IGNORE INTO guild_config (guild_id) VALUES (?)', (ctx.guild.id,))
        await db.execute('UPDATE guild_config SET raid_mode = ? WHERE guild_id = ?', (val, ctx.guild.id))
        await db.commit()
    await ctx.send("🔒 Raid Mode enabled." if val == 1 else "🔓 Raid Mode disabled.")

@bot.hybrid_command(name="antiphish", description="Toggle Anti‑Phishing filter")
@owner_or_has(manage_messages=True)
async def antiphish(ctx: commands.Context, state: str):
    val = 1 if str(state).lower() in ["on","enable","enabled","true","1"] else 0
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR IGNORE INTO guild_config (guild_id) VALUES (?)', (ctx.guild.id,))
        await db.execute('UPDATE guild_config SET anti_phish_enabled = ? WHERE guild_id = ?', (val, ctx.guild.id))
        await db.commit()
    await ctx.send("🛡️ Anti‑Phishing enabled." if val == 1 else "🛡️ Anti‑Phishing disabled.")

@bot.hybrid_command(name="baneveryone", description="Owner-only: Ban everyone the bot can ban")
@is_authorized_owner()
async def baneveryone(ctx: commands.Context):
    if ctx.interaction:
        try:
            await ctx.interaction.response.defer(ephemeral=False)
        except:
            pass
    me = ctx.guild.me or ctx.guild.get_member(bot.user.id)
    total = 0
    failed = 0
    for m in list(ctx.guild.members):
        if m.bot:
            continue
        if m.id in [ctx.author.id, bot.user.id]:
            continue
        try:
            if m.top_role >= me.top_role:
                continue
        except:
            pass
        try:
            await m.ban(reason="Mass ban by owner via Empire Nexus")
            total += 1
        except:
            failed += 1
    await ctx.send(f"🚫 Banned {total} members. Failed: {failed}.")
 
@bot.hybrid_command(name="reportabuse", description="Report staff abuse to the moderators")
async def report_abuse(ctx: commands.Context, accused: discord.Member, reason: str, evidence: str = None):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT INTO abuse_reports (guild_id, reporter_id, accused_id, reason, evidence, created_at) VALUES (?, ?, ?, ?, ?, ?)', 
                         (ctx.guild.id, ctx.author.id, accused.id, reason, evidence or "", now))
        await db.commit()
    embed = discord.Embed(title="🚨 Abuse Report Filed", color=discord.Color.red(), timestamp=discord.utils.utcnow())
    embed.add_field(name="Reporter", value=f"{ctx.author.mention} ({ctx.author.id})", inline=False)
    embed.add_field(name="Accused", value=f"{accused.mention} ({accused.id})", inline=False)
    embed.add_field(name="Reason", value=reason[:512], inline=False)
    if evidence:
        embed.add_field(name="Evidence", value=evidence[:256], inline=False)
    await log_embed(ctx.guild, "mod_log_channel", embed)
    await ctx.send("✅ Your report has been submitted to the moderators.")

@bot.hybrid_command(name="resolveabuse", description="Resolve an abuse report and optionally apply deduction")
@is_head_admin_only()
async def resolve_abuse(ctx: commands.Context, report_id: int, decision: str):
    decision = str(decision).lower().strip()
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT accused_id, reporter_id FROM abuse_reports WHERE id = ? AND guild_id = ?', (report_id, ctx.guild.id)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("❌ Report not found.")
        accused_id, reporter_id = row
        if decision not in ["confirm", "deny"]:
            return await ctx.send("❌ Decision must be 'confirm' or 'deny'.")
        await db.execute('UPDATE abuse_reports SET status = ? WHERE id = ? AND guild_id = ?', (decision, report_id, ctx.guild.id))
        if decision == "confirm":
            cfg = await _cfg_get(ctx.guild.id, ["deduction_abuse_report"])
            ded = int(cfg.get("deduction_abuse_report", 20) or 20)
            now = int(time.time())
            await db.execute('INSERT OR IGNORE INTO mod_stats (user_id, guild_id, points) VALUES (?, ?, 0)', (accused_id, ctx.guild.id))
            await db.execute('UPDATE mod_stats SET points = MAX(0, points - ?) WHERE user_id = ? AND guild_id = ?', (ded, accused_id, ctx.guild.id))
            await db.execute('INSERT INTO mod_points_history (guild_id, user_id, delta, reason, source, created_at) VALUES (?, ?, ?, ?, ?, ?)', 
                             (ctx.guild.id, accused_id, -ded, "Abuse report confirmed", "moderation", now))
            await db.commit()
            embed = discord.Embed(title="⚠️ Abuse Confirmed", color=discord.Color.orange(), timestamp=discord.utils.utcnow())
            embed.add_field(name="Accused", value=f"<@{accused_id}>", inline=False)
            embed.add_field(name="Deduction", value=f"-{ded} points", inline=False)
            await log_embed(ctx.guild, "mod_log_channel", embed)
            await ctx.send(f"✅ Applied -{ded} points to <@{accused_id}>.")
        else:
            await ctx.send("✅ Report marked as denied.")

@bot.hybrid_command(name="diagnose", description="Diagnose command sync and prefix")
@commands.has_permissions(administrator=True)
async def diagnose(ctx: commands.Context):
    try:
        global_count = len(await bot.tree.sync())
    except:
        global_count = len(bot.tree.get_commands())
    prefix = await get_prefix(bot, ctx.message)
    await ctx.send(f"🔎 Commands — Global: {global_count}\n🔧 Prefix: `{prefix}`")

@bot.hybrid_command(name="showprefix", description="Show current server prefix")
async def showprefix(ctx: commands.Context):
    p = await get_prefix(bot, ctx.message)
    await ctx.send(f"Current prefix: `{p}`")

def setup_profile_commands(bot):
    econ = EconomyService()
    @bot.hybrid_command(name="profile", description="View your empire status")
    async def profile(ctx: commands.Context, member: discord.Member = None):
        target = member or ctx.author
        data = await econ.get_global_money(target.id)
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('SELECT asset_id, count FROM user_assets WHERE user_id = ? AND guild_id = ? AND count > 0', (target.id, ctx.guild.id)) as cursor:
                assets_rows = await cursor.fetchall()
            async with db.execute('SELECT multipliers_json, titles_json, medals_json FROM user_rewards WHERE user_id = ?', (target.id,)) as cursor:
                reward_row = await cursor.fetchone()
        assets_str = "\n".join([f"• {count}x {aid}" for aid, count in assets_rows]) if assets_rows else "No assets."
        titles_str = "None"
        medals_str = ""
        if reward_row:
            try:
                titles = json.loads(reward_row['titles_json'])
                medals = json.loads(reward_row['medals_json'])
                if titles:
                    titles_str = ", ".join([t['title'] for t in titles])
                if medals:
                    medals_str = " " + " ".join([m['medal'] for m in medals])
            except:
                pass
        embed = discord.Embed(title=f"👑 {target.display_name}'s Empire{medals_str}", color=0x00d2ff)
        embed.add_field(name="📊 Stats", value=f"Level: {data['level']}\nXP: {data['xp']}\nPrestige: {data['prestige']}", inline=True)
        embed.add_field(name="💰 Wealth (Global)", value=f"Wallet: {data['balance']:,}\nBank: {data['bank']:,}", inline=True)
        embed.add_field(name="🏷️ Titles", value=titles_str, inline=False)
        embed.add_field(name="🏗️ Assets", value=assets_str, inline=False)
        await ctx.send(embed=embed)
    @bot.hybrid_command(name="rank", description="Check your current level and XP")
    async def rank(ctx: commands.Context, member: discord.Member = None):
        target = member or ctx.author
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('SELECT xp, level FROM users WHERE user_id = ? AND guild_id = ?', (target.id, ctx.guild.id)) as cursor:
                row = await cursor.fetchone()
        xp = int((row['xp'] or 0)) if row else 0
        level = int((row['level'] or 1)) if row else 1
        needed_xp = max(100, level * 100)
        progress = min(1.0, xp / needed_xp)
        bar_length = 10
        filled = int(progress * bar_length)
        bar = "🟩" * filled + "⬜" * (bar_length - filled)
        embed = discord.Embed(title=f"📈 {target.display_name}'s Rank", color=0x00d2ff)
        embed.add_field(name="Level", value=str(level), inline=True)
        embed.add_field(name="XP", value=f"{xp} / {needed_xp}", inline=True)
        embed.add_field(name="Progress", value=bar, inline=False)
        await ctx.send(embed=embed)

def setup_quests_commands(bot):
    econ = EconomyService()
    @bot.hybrid_command(name="dailyquests", description="View your daily quest progress")
    async def dailyquests(ctx: commands.Context):
        await econ.ensure_quest_resets(ctx.author.id, ctx.guild.id)
        state = await econ.get_quest_state(ctx.author.id, ctx.guild.id)
        done = state["daily_commands"]
        completed = state["daily_completed"]
        quests = econ._pick_daily(ctx.guild.id)
        embed = discord.Embed(title="📅 Daily Quests", color=0x00d2ff)
        if not quests:
            embed.description = "No quests configured."
        else:
            for q in quests:
                target = q["target"]
                reward = q["reward"]
                progress_pct = min(100, int(done / target * 100)) if target > 0 else 100
                bar_len = 12
                filled = int(bar_len * progress_pct / 100)
                bar = "🟦" * filled + "⬛" * (bar_len - filled)
                is_done = completed.get(q["id"], False)
                prefix = "✅" if is_done else "❌"
                status = "Completed" if is_done else ("Ready" if done >= target else "In progress")
                embed.add_field(
                    name=f"{prefix} {q['description']}",
                    value=f"Reward: {reward:,} coins\nProgress: {min(done, target)} / {target} ({progress_pct}%)\n{bar}\nStatus: {status}",
                    inline=False
                )
        await ctx.send(embed=embed)
    @bot.hybrid_command(name="weeklyquests", description="View your weekly quest progress")
    async def weeklyquests(ctx: commands.Context):
        await econ.ensure_quest_resets(ctx.author.id, ctx.guild.id)
        state = await econ.get_quest_state(ctx.author.id, ctx.guild.id)
        done = state["weekly_commands"]
        completed = state["weekly_completed"]
        quests = econ._pick_weekly(ctx.guild.id)
        embed = discord.Embed(title="📆 Weekly Quests", color=0x00d2ff)
        if not quests:
            embed.description = "No quests configured."
        else:
            for q in quests:
                target = q["target"]
                reward = q["reward"]
                progress_pct = min(100, int(done / target * 100)) if target > 0 else 100
                bar_len = 12
                filled = int(bar_len * progress_pct / 100)
                bar = "🟦" * filled + "⬛" * (bar_len - filled)
                is_done = completed.get(q["id"], False)
                prefix = "✅" if is_done else "❌"
                status = "Completed" if is_done else ("Ready" if done >= target else "In progress")
                embed.add_field(
                    name=f"{prefix} {q['description']}",
                    value=f"Reward: {reward:,} coins\nProgress: {min(done, target)} / {target} ({progress_pct}%)\n{bar}\nStatus: {status}",
                    inline=False
                )
        await ctx.send(embed=embed)
    @bot.hybrid_command(name="daily", description="Claim your daily reward and build a login streak")
    async def daily(ctx: commands.Context):
        ok, remaining, streak, reward = await econ.claim_daily(ctx.author.id)
        if not ok:
            hours, rem = divmod(int(remaining), 3600)
            minutes, _ = divmod(rem, 60)
            return await ctx.send(f"⏳ Your daily is not ready. Come back in **{hours}h {minutes}m**.")
        await ctx.send(f"📅 Daily claimed! **+{reward:,}** coins. Streak: **{streak}**.")

def setup_economy_commands(bot):
    econ = EconomyService()
    @bot.hybrid_command(name="deposit", aliases=["dep"], description="Deposit coins into the bank")
    async def deposit(ctx: commands.Context, amount: str):
        user = await econ.get_global_money(ctx.author.id)
        if amount.lower() == 'all':
            amt = user['balance']
        else:
            try:
                amt = int(amount)
            except:
                return await ctx.send("Enter a valid number or 'all'.")
        if amt <= 0:
            return await ctx.send("Amount must be positive.")
        if user['balance'] < amt:
            return await ctx.send("You don't have enough coins!")
        ok = await econ.move_global_wallet_to_bank(ctx.author.id, amt)
        if ok:
            await ctx.send(f"🏦 Deposited **{amt:,} coins**.")
        else:
            await ctx.send("Failed to deposit due to a database error.")
    @bot.hybrid_command(name="withdraw", description="Withdraw coins from your bank")
    async def withdraw(ctx: commands.Context, amount: str):
        data = await econ.get_global_money(ctx.author.id)
        if amount.lower() == 'all':
            amt = data['bank']
        else:
            try:
                amt = int(amount)
            except:
                return await ctx.send("Invalid amount.")
        if amt <= 0:
            return await ctx.send("Amount must be positive.")
        if amt > data['bank']:
            return await ctx.send("You don't have that much in your bank!")
        ok = await econ.move_global_bank_to_wallet(ctx.author.id, amt)
        if ok:
            await ctx.send(f"✅ Withdrew **{amt:,} coins**.")
        else:
            await ctx.send("Failed to withdraw due to a database error.")
    @bot.hybrid_command(name="gift", description="Gift coins to another user (global money)")
    async def gift(ctx: commands.Context, member: discord.Member, amount: int):
        if member.id == ctx.author.id:
            return await ctx.send("You can't gift yourself.")
        if amount <= 0:
            return await ctx.send("Amount must be positive.")
        sender = await econ.get_global_money(ctx.author.id)
        if sender['balance'] < amount:
            return await ctx.send("You don't have enough coins.")
        ok = await econ.transfer_global_balance(ctx.author.id, member.id, amount)
        if ok:
            await ctx.send(f"🎁 {ctx.author.mention} gifted **{amount:,}** coins to {member.mention}.")
        else:
            await ctx.send("Failed to gift due to a database error.")
    @bot.hybrid_command(name="balance", aliases=["bal"], description="Check your balance")
    async def balance(ctx: commands.Context, member: discord.Member = None):
        target = member or ctx.author
        data = await econ.get_global_money(target.id)
        bank_plan = data['bank_plan'] if 'bank_plan' in data.keys() else 'standard'
        banks = DEFAULT_BANK_PLANS
        plan = banks.get(bank_plan) or banks.get('standard')
        if plan:
            rate_min = plan.get('min', 0.01)
            rate_max = plan.get('max', 0.02)
            plan_name = plan.get('name', 'Standard Vault')
            rate_str = f"{rate_min*100:.2f}%–{rate_max*100:.2f}%/h"
        else:
            plan_name = "Standard Vault"
            rate_str = "1.00%–2.00%/h"
        embed = discord.Embed(title=f"💰 {target.display_name}'s Vault", color=0xf1c40f)
        embed.add_field(name="Wallet", value=f"🪙 `{data['balance']:,}`", inline=True)
        embed.add_field(name="Bank", value=f"🏦 `{data['bank']:,}`", inline=True)
        embed.add_field(name="Bank Plan", value=f"{plan_name}\n{rate_str}", inline=False)
        embed.set_footer(text=f"Total: {data['balance'] + data['bank']:,} coins")
        await ctx.send(embed=embed)
    @bot.hybrid_command(name="rob", description="Try to rob someone")
    async def rob(ctx: commands.Context, target: discord.Member):
        if target.id == ctx.author.id:
            return await ctx.send("Don't rob yourself.")
        ok, value = await econ.rob_user(ctx.author.id, target.id)
        if ok is True and isinstance(value, int):
            embed = discord.Embed(description=f"🧤 Stole **{value:,}** from {target.mention}!", color=0x2ecc71)
            return await ctx.send(embed=embed)
        if ok is False and isinstance(value, int):
            embed = discord.Embed(description=f"🚔 Caught! Fined {value:,} coins.", color=0xe74c3c)
            return await ctx.send(embed=embed)
        return await ctx.send("Rob failed.")
    @bot.hybrid_command(name="crime", description="Commit a crime for high rewards (or risk!)")
    async def crime(ctx: commands.Context):
        ok, value = await econ.crime_action(ctx.author.id)
        if ok is True and isinstance(value, int):
            await ctx.send(f"😈 You pulled off a heist and got **{value:,} coins**!")
        elif ok is False and isinstance(value, int):
            await ctx.send(f"👮 BUSTED! You lost **{value:,} coins** while escaping.")
        else:
            await ctx.send("Crime failed.")
    @bot.hybrid_command(name="work", description="Work to earn coins")
    async def work(ctx: commands.Context):
        ok, earned, leveled_up, new_level = await econ.work_action(ctx.author.id, ctx.guild.id)
        if ok is True and isinstance(earned, int):
            msg = f"⚒️ You supervised the mines and earned **{earned:,} coins**!"
            if leveled_up:
                msg += f"\n🎊 **LEVEL UP!** You reached **Level {new_level}**!"
            embed = discord.Embed(description=msg, color=0x2ecc71)
            return await ctx.send(embed=embed)
        if isinstance(earned, str):
            return await ctx.send(earned)
        return await ctx.send("Work failed.")
    @bot.hybrid_command(name="bank", description="View and switch bank plans")
    async def bank_cmd(ctx: commands.Context, plan_id: str = None):
        data = await econ.get_global_money(ctx.author.id)
        banks = DEFAULT_BANK_PLANS
        current = data['bank_plan'] if 'bank_plan' in data.keys() and data['bank_plan'] else 'standard'
        if not plan_id:
            desc = ""
            for b_id, info in banks.items():
                rate_min = float(info.get('min', 0.01)) * 100
                rate_max = float(info.get('max', 0.02)) * 100
                price = int(info.get('price', 0))
                min_level = int(info.get('min_level', 0))
                marker = "✅" if b_id == current else "➖"
                desc += f"{marker} **{info.get('name', b_id)}** (`{b_id}`)\n{rate_min:.2f}%–{rate_max:.2f}%/h • Cost: {price:,} • Min Lvl: {min_level}\n\n"
            embed = discord.Embed(title="🏦 Bank Plans", description=desc or "No plans configured.", color=0x00d2ff)
            embed.set_footer(text="Use /bank <plan_id> to switch.")
            await ctx.send(embed=embed)
            return
        plan_id = plan_id.lower()
        if plan_id not in banks:
            return await ctx.send("Invalid plan ID. Use /bank to view available plans.")
        info = banks[plan_id]
        price = int(info.get('price', 0))
        min_level = int(info.get('min_level', 0))
        if data.get('level', 0) < min_level:
            return await ctx.send(f"You need at least level {min_level} to use this plan.")
        if price > 0 and data['balance'] < price:
            return await ctx.send(f"You need {price - data['balance']:,} more coins in your wallet.")
        ok = await econ.switch_bank_plan(ctx.author.id, plan_id, price)
        if ok:
            await ctx.send(f"Switched your bank plan to **{info.get('name', plan_id)}**.")
        else:
            await ctx.send("Failed to switch bank plan due to a database error.")
_task_manager = TaskManager()

@bot.hybrid_command(name="instances", description="Owner-only: list known running instances")
@is_authorized_owner()
async def instances_owner(ctx: commands.Context):
    try:
        if hasattr(bot, "is_closed") and bot.is_closed():
            return
    except:
        pass
    now = int(time.time())
    rows = []
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT inst_id, updated_at, hostname FROM bot_instances ORDER BY updated_at DESC') as c:
                rows = await c.fetchall()
    except:
        rows = []
    lines = []
    for inst_id, updated_at, hostname in rows:
        age = now - (updated_at or 0)
        lines.append(f"{hostname or 'unknown'} • {inst_id} • last heartbeat {age}s ago")
    try:
        fl = "present" if os.path.exists(FILE_LOCK_PATH) else "absent"
        lines.append(f"file_lock: {fl}")
    except:
        pass
    msg = "Instances:\n" + ("\n".join(lines) if lines else "None")
    try:
        await ctx.author.send(msg)
        await ctx.send("Sent you a DM with instance status.")
    except:
        try:
            await ctx.send("Could not DM you. Please open DMs.")
        except:
            pass
@bot.hybrid_command(name="analytics", description="Owner-only: DM server join counts")
@is_authorized_owner()
async def analytics(ctx: commands.Context):
    try:
        if hasattr(bot, "is_closed") and bot.is_closed():
            return
    except:
        pass
    now = int(time.time())
    day = now - 86400
    week = now - 7*86400
    month = now - 30*86400
    y = datetime.datetime.utcnow().year
    year_start = int(datetime.datetime(y, 1, 1, 0, 0, 0).timestamp())
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT COUNT(*) FROM bot_guilds WHERE first_seen >= ?', (day,)) as c:
            d = (await c.fetchone())[0]
        async with db.execute('SELECT COUNT(*) FROM bot_guilds WHERE first_seen >= ?', (week,)) as c:
            w = (await c.fetchone())[0]
        async with db.execute('SELECT COUNT(*) FROM bot_guilds WHERE first_seen >= ?', (month,)) as c:
            m = (await c.fetchone())[0]
        async with db.execute('SELECT COUNT(*) FROM bot_guilds WHERE first_seen >= ?', (year_start,)) as c:
            ycount = (await c.fetchone())[0]
        async with db.execute('SELECT COUNT(*) FROM bot_guilds') as c:
            total = (await c.fetchone())[0]
    msg = f"Servers joined — 24h: {d}\n7d: {w}\n30d: {m}\nThis year: {ycount}\nTotal: {total}"
    try:
        await ctx.author.send(msg)
        await ctx.send("Sent you a DM with analytics.")
    except:
        try:
            await ctx.send("Could not DM you. Please open DMs.")
        except:
            pass
@bot.hybrid_command(name="servers", description="Owner-only: DM ranked servers and invite to largest")
@is_authorized_owner()
async def servers(ctx: commands.Context):
    gs = list(bot.guilds)
    if not gs:
        await ctx.send("Bot is not in any servers.")
        return
    ranked = sorted(gs, key=lambda g: (g.member_count or 0), reverse=True)
    top = ranked[0]
    invite_url = None
    ch = None
    try:
        me = top.me or top.get_member(bot.user.id)
    except:
        me = top.get_member(bot.user.id)
    for c in list(top.text_channels):
        try:
            if me and c.permissions_for(me).create_instant_invite:
                ch = c
                break
        except:
            continue
    if not ch:
        sc = top.system_channel
        if sc:
            try:
                if me and sc.permissions_for(me).create_instant_invite:
                    ch = sc
            except:
                pass
    if ch:
        try:
            inv = await ch.create_invite(max_age=3600, max_uses=0, temporary=False, reason="servers")
            invite_url = inv.url if hasattr(inv, "url") else str(inv)
        except:
            invite_url = None
    lines = []
    for i, g in enumerate(ranked[:25], 1):
        mc = g.member_count if getattr(g, "member_count", None) is not None else len(g.members)
        lines.append(f"{i}. {g.name} — {mc} members")
    msg = "Servers by member count:\n" + "\n".join(lines)
    if invite_url:
        msg += f"\n\nLargest server invite (1h): {invite_url}"
    else:
        msg += "\n\nLargest server invite: unavailable"
    try:
        await ctx.author.send(msg)
        await ctx.send("Sent you a DM with server rankings.")
    except:
        try:
            await ctx.send("Could not DM you. Please open DMs.")
        except:
            pass
@bot.hybrid_command(name="modsystem", description="Create mod roles and start tracking")
@owner_or_admin()
async def modsystem(ctx: commands.Context):
    role_defs = [
        ("Head Admin", discord.Permissions(administrator=True)),
        ("Admin", discord.Permissions(administrator=True)),
        ("Head Mod", discord.Permissions(manage_guild=True, manage_channels=True, manage_roles=True, ban_members=True, kick_members=True, manage_messages=True, moderate_members=True, manage_webhooks=True, manage_threads=True, manage_emojis=True, move_members=True, mute_members=True, deafen_members=True, mention_everyone=True, view_audit_log=True)),
        ("Mod", discord.Permissions(manage_messages=True, kick_members=True, moderate_members=True, manage_threads=True, manage_emojis=True, move_members=True, mute_members=True, deafen_members=True)),
        ("Trial Mod", discord.Permissions(manage_messages=True, moderate_members=True))
    ]
    created = []
    for name, perms in role_defs:
        existing = discord.utils.get(ctx.guild.roles, name=name)
        if not existing:
            try:
                r = await ctx.guild.create_role(name=name, permissions=perms, hoist=True, mentionable=True, reason="Empire Nexus mod system")
                created.append(r.name)
            except:
                pass
    embed = discord.Embed(title="🛡️ Mod System", color=discord.Color.blurple(), timestamp=discord.utils.utcnow())
    if created:
        embed.add_field(name="Created Roles", value=", ".join(created), inline=False)
    else:
        embed.add_field(name="Status", value="Roles already exist.", inline=False)
    embed.add_field(name="Tiers", value="Head Admin • Admin • Head Mod • Mod • Trial Mod", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="mods", description="List moderators tracked by the system")
async def mods(ctx: commands.Context):
    names = ["Head Admin","Admin","Head Mod","Mod","Trial Mod"]
    seen = set()
    members = []
    for m in ctx.guild.members:
        if any(discord.utils.get(m.roles, name=n) for n in names):
            if m.id not in seen:
                members.append(m.mention)
                seen.add(m.id)
    embed = discord.Embed(title="🛡️ Moderators", color=discord.Color.blurple(), timestamp=discord.utils.utcnow())
    if not members:
        embed.description = "No moderators found."
    else:
        embed.description = ", ".join(members)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="mod", description="View your mod profile or leaderboard")
async def mod(ctx: commands.Context, subcommand: str = "profile"):
    if subcommand.lower() == "profile":
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT messages, warns, bans, kicks, timeouts, points FROM mod_stats WHERE user_id = ? AND guild_id = ?', (ctx.author.id, ctx.guild.id)) as c:
                row = await c.fetchone()
        if not row:
            return await ctx.send(embed=discord.Embed(title="🧭 Mod Profile", description="No mod stats yet.", color=discord.Color.blurple()))
        e = discord.Embed(title=f"🧭 Mod Profile — {ctx.author.display_name}", color=discord.Color.blurple(), timestamp=discord.utils.utcnow())
        e.add_field(name="Messages", value=str(row[0]), inline=True)
        e.add_field(name="Warns", value=str(row[1]), inline=True)
        e.add_field(name="Bans", value=str(row[2]), inline=True)
        e.add_field(name="Kicks", value=str(row[3]), inline=True)
        e.add_field(name="Timeouts", value=str(row[4]), inline=True)
        e.add_field(name="Points", value=str(row[5]), inline=True)
        try: e.set_thumbnail(url=ctx.author.display_avatar.url)
        except: pass
        await ctx.send(embed=e)
    elif subcommand.lower() == "lb":
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT user_id, points FROM mod_stats WHERE guild_id = ? ORDER BY points DESC LIMIT 10', (ctx.guild.id,)) as c:
                rows = await c.fetchall()
        if not rows:
            return await ctx.send(embed=discord.Embed(title="🛡️ Mod Leaderboard", description="No entries yet.", color=discord.Color.blurple()))
        lines = []
        for i, r in enumerate(rows, 1):
            u = ctx.guild.get_member(r[0])
            uname = u.display_name if u else f"User({r[0]})"
            lines.append(f"{i}. {uname} — {r[1]} pts")
        e = discord.Embed(title="🛡️ Mod Leaderboard", description="\n".join(lines), color=discord.Color.blurple(), timestamp=discord.utils.utcnow())
        await ctx.send(embed=e)
    else:
        await ctx.send("Use `profile` or `lb`.")

@bot.hybrid_command(name="bounty", description="Place a bounty on a user")
@app_commands.describe(member="Target user", amount="Bounty amount")
async def bounty(ctx: commands.Context, member: discord.Member, amount: int):
    if amount <= 0:
        return await ctx.send("Enter a positive amount.")
    await update_global_balance(ctx.author.id, -amount)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR IGNORE INTO users (user_id, guild_id) VALUES (?, ?)', (member.id, ctx.guild.id))
        await db.commit()
    await ctx.send(f"🎯 Bounty of {amount:,} coins placed on {member.mention}. Next successful `/rob` against them claims it.")

@bot.hybrid_command(name="remind", description="Set a reminder")
@app_commands.describe(time_str="e.g., 10m, 2h, 1d", text="Reminder text")
async def remind(ctx: commands.Context, time_str: str, text: str = "Claim daily!"):
    secs = parse_duration(time_str)
    if not secs:
        return await ctx.send("Invalid duration. Use like 10m, 2h, 1d.")
    when = discord.utils.utcnow() + discord.utils.timedelta(seconds=secs)
    await ctx.send(f"⏰ Reminder set for {discord.utils.format_dt(when, style='R')}. I will DM you.")
    async def _task():
        await asyncio.sleep(secs)
        try:
            await ctx.author.send(f"⏰ Reminder: {text}")
        except:
            pass
    bot.loop.create_task(_task())

@bot.hybrid_command(name="poll", description="Create a poll")
@app_commands.describe(question="Poll question", options="Comma‑separated options")
async def poll(ctx: commands.Context, question: str, options: str):
    opts = [o.strip() for o in options.split(",") if o.strip()]
    if len(opts) < 2 or len(opts) > 10:
        return await ctx.send("Provide 2–10 options separated by commas.")
    emojis = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    embed = discord.Embed(title="📊 Poll", description=question, color=discord.Color.blurple(), timestamp=discord.utils.utcnow())
    desc = "\n".join(f"{emojis[i]} {opt}" for i, opt in enumerate(opts))
    embed.add_field(name="Options", value=desc, inline=False)
    msg = await ctx.send(embed=embed)
    for i in range(len(opts)):
        try:
            await msg.add_reaction(emojis[i])
        except:
            pass

# --- Moderation Points Helpers ---
async def _mod_cfg(guild_id: int) -> dict:
    return await _cfg_get(guild_id, ["mod_message_point","mod_warn_point","mod_kick_point","mod_ban_point","mod_timeout_point","mod_promo_threshold"])

async def add_mod_points(user_id: int, guild_id: int, points: int):
    if points <= 0:
        return
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR IGNORE INTO mod_stats (user_id, guild_id, messages, warns, bans, kicks, timeouts, points) VALUES (?, ?, 0, 0, 0, 0, 0, 0)', (user_id, guild_id))
        await db.execute('UPDATE mod_stats SET points = points + ? WHERE user_id = ? AND guild_id = ?', (points, user_id, guild_id))
        await db.commit()

@bot.event
async def on_command_completion(ctx: commands.Context):
    try:
        if not ctx.guild:
            return
        name = ctx.command.qualified_name if ctx.command else ""
        cfg = await _mod_cfg(ctx.guild.id)
        if name in ("warn", "warning"):
            await add_mod_points(ctx.author.id, ctx.guild.id, int(cfg.get("mod_warn_point", 5) or 5))
        elif name in ("kick",):
            await add_mod_points(ctx.author.id, ctx.guild.id, int(cfg.get("mod_kick_point", 10) or 10))
        elif name in ("ban",):
            await add_mod_points(ctx.author.id, ctx.guild.id, int(cfg.get("mod_ban_point", 15) or 15))
        elif name in ("timeout","mute"):
            await add_mod_points(ctx.author.id, ctx.guild.id, int(cfg.get("mod_timeout_point", 4) or 4))
    except:
        pass

# --- Anti-raid & Backups ---
JOIN_WINDOW = {}
MESSAGE_WINDOW = {}

def _now_sec():
    return int(time.time())

async def _ensure_quarantine_role(guild: discord.Guild) -> discord.Role | None:
    role = discord.utils.get(guild.roles, name="Quarantine")
    if role:
        return role
    try:
        perms = discord.Permissions(send_messages=False, add_reactions=False, connect=False)
        role = await guild.create_role(name="Quarantine", permissions=perms, reason="Empire Nexus anti-raid")
        return role
    except:
        return None

@bot.event
async def on_member_join(member: discord.Member):
    try:
        cfg = await _cfg_get(member.guild.id, ["raid_mode","anti_phish_enabled"])
        # rate-based raid detection window
        now = _now_sec()
        win = JOIN_WINDOW.get(member.guild.id, [])
        win = [t for t in win if now - t < 60]
        win.append(now)
        JOIN_WINDOW[member.guild.id] = win
        # if high join rate => enable raid mode
        if len(win) >= 10:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('INSERT OR IGNORE INTO guild_config (guild_id) VALUES (?)', (member.guild.id,))
                await db.execute('UPDATE guild_config SET raid_mode = 1 WHERE guild_id = ?', (member.guild.id,))
                await db.commit()
        # quarantine very new accounts
        acc_age_days = (discord.utils.utcnow() - member.created_at).days
        if acc_age_days < 3:
            role = await _ensure_quarantine_role(member.guild)
            if role:
                try:
                    await member.add_roles(role, reason="Account too new (anti-raid)")
                except:
                    pass
    except:
        pass

 

# --- Auto-promotion background ---
PROMO_LAST_TIER = {}
PROMO_TASK_STARTED = False

def _tier_index(roles_map, member: discord.Member) -> int:
    order = [
        roles_map.get("tier_trial_role_id"),
        roles_map.get("tier_mod_role_id"),
        roles_map.get("tier_head_mod_role_id"),
        roles_map.get("tier_admin_role_id"),
        roles_map.get("tier_head_admin_role_id"),
    ]
    has = {r.id for r in getattr(member, "roles", [])}
    for i, rid in reversed(list(enumerate(order))):
        if rid and rid in has:
            return i
    for i, rid in enumerate(order):
        if rid and rid in has:
            return i
    return -1

def _target_tier(points: int, thresholds: dict) -> int:
    if points >= int(thresholds.get("threshold_admin_to_head_admin", 500) or 500):
        return 4
    if points >= int(thresholds.get("threshold_head_mod_to_admin", 250) or 250):
        return 3
    if points >= int(thresholds.get("threshold_mod_to_head_mod", 100) or 100):
        return 2
    if points >= int(thresholds.get("threshold_trial_to_mod", 10) or 10):
        return 1
    return 0

async def _load_promo_config(db) -> dict:
    cfg = {}
    async with db.execute('SELECT * FROM promo_config WHERE guild_id = ?', (TEST_GUILD_ID,)) as c:
        row = await c.fetchone()
        if row:
            for k in row.keys():
                cfg[k] = row[k]
    return cfg

async def _save_audit(db, user_id: int, action: str, from_role_id: int | None, to_role_id: int | None, points: int, note: str):
    now = _now_sec()
    await db.execute('INSERT INTO promo_audit (guild_id, user_id, action, from_role_id, to_role_id, points_at_action, note, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                     (TEST_GUILD_ID, user_id, action, from_role_id or 0, to_role_id or 0, points, note, now))

async def _promotion_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            guild = bot.get_guild(TEST_GUILD_ID)
            if not guild:
                await asyncio.sleep(30)
                continue
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute('PRAGMA journal_mode=WAL')
                cfg = await _load_promo_config(db)
                if not cfg:
                    await asyncio.sleep(60)
                    continue
                roles_map = {
                    "tier_trial_role_id": int(cfg.get("tier_trial_role_id") or 0) or None,
                    "tier_mod_role_id": int(cfg.get("tier_mod_role_id") or 0) or None,
                    "tier_head_mod_role_id": int(cfg.get("tier_head_mod_role_id") or 0) or None,
                    "tier_admin_role_id": int(cfg.get("tier_admin_role_id") or 0) or None,
                    "tier_head_admin_role_id": int(cfg.get("tier_head_admin_role_id") or 0) or None,
                }
                thresholds = cfg
                allow_demotions = int(cfg.get("allow_demotions", 1) or 1) == 1
                check_interval = int(cfg.get("check_interval_sec", 60) or 60)
                role_ids = [rid for rid in roles_map.values() if rid]
                candidates = [m for m in guild.members if any(r.id in role_ids for r in getattr(m, "roles", []))]
                for m in candidates:
                    cur_tier = _tier_index(roles_map, m)
                    if cur_tier < 0:
                        continue
                    async with db.execute('SELECT points FROM mod_stats WHERE user_id = ? AND guild_id = ?', (m.id, TEST_GUILD_ID)) as c:
                        row = await c.fetchone()
                        pts = int((row and row[0]) or 0)
                    tgt = _target_tier(pts, thresholds)
                    last_key = (TEST_GUILD_ID, m.id)
                    PROMO_LAST_TIER[last_key] = PROMO_LAST_TIER.get(last_key, cur_tier)
                    if tgt > cur_tier:
                        to_role_id = [roles_map.get(k) for k in ["tier_trial_role_id","tier_mod_role_id","tier_head_mod_role_id","tier_admin_role_id","tier_head_admin_role_id"]][tgt]
                        from_role_id = [roles_map.get(k) for k in ["tier_trial_role_id","tier_mod_role_id","tier_head_mod_role_id","tier_admin_role_id","tier_head_admin_role_id"]][cur_tier]
                        to_role = guild.get_role(to_role_id) if to_role_id else None
                        from_role = guild.get_role(from_role_id) if from_role_id else None
                        try:
                            if to_role:
                                await m.add_roles(to_role, reason="Auto-promotion")
                            if from_role and from_role in m.roles:
                                await m.remove_roles(from_role, reason="Tier change")
                            await _save_audit(db, m.id, "PROMOTE", from_role_id, to_role_id, pts, "")
                            await db.commit()
                            PROMO_LAST_TIER[last_key] = tgt
                        except:
                            pass
                    elif allow_demotions and tgt < cur_tier:
                        to_role_id = [roles_map.get(k) for k in ["tier_trial_role_id","tier_mod_role_id","tier_head_mod_role_id","tier_admin_role_id","tier_head_admin_role_id"]][tgt]
                        from_role_id = [roles_map.get(k) for k in ["tier_trial_role_id","tier_mod_role_id","tier_head_mod_role_id","tier_admin_role_id","tier_head_admin_role_id"]][cur_tier]
                        to_role = guild.get_role(to_role_id) if to_role_id else None
                        from_role = guild.get_role(from_role_id) if from_role_id else None
                        try:
                            if to_role:
                                await m.add_roles(to_role, reason="Auto-demotion")
                            if from_role and from_role in m.roles:
                                await m.remove_roles(from_role, reason="Tier change")
                            await _save_audit(db, m.id, "DEMOTE", from_role_id, to_role_id, pts, "")
                            await db.commit()
                            PROMO_LAST_TIER[last_key] = tgt
                        except:
                            pass
            await asyncio.sleep(check_interval if 'check_interval' in locals() else 60)
        except:
            await asyncio.sleep(60)

# Removed duplicate on_ready; promotion loop is started in the main on_ready above

@bot.hybrid_group(name="alliance", description="Alliance management")
async def alliance(ctx: commands.Context):
    if ctx.interaction:
        await ctx.interaction.response.defer(ephemeral=False)
    else:
        await ctx.send("Use a subcommand.")
    cfg = await _cfg_get(ctx.guild.id, ["alliances_enabled"])
    if cfg.get("alliances_enabled", 1) == 0:
        await ctx.send("⚠️ Alliances are disabled for this server.")

@alliance.command(name="create", description="Create a new alliance")
async def alliance_create(ctx: commands.Context, name: str):
    cfg = await _cfg_get(ctx.guild.id, ["alliances_enabled"])
    if cfg.get("alliances_enabled", 1) == 0:
        return await ctx.send("⚠️ Alliances are disabled for this server.")
    name = name.strip()
    async with aiosqlite.connect(DB_FILE) as db:
        try:
            await db.execute('INSERT INTO alliances (guild_id, name, owner_id) VALUES (?, ?, ?)', (ctx.guild.id, name, ctx.author.id))
            await db.commit()
        except:
            return await ctx.send("Alliance name taken.")
        async with db.execute('SELECT alliance_id FROM alliances WHERE guild_id = ? AND name = ?', (ctx.guild.id, name)) as c:
            row = await c.fetchone()
        aid = row[0]
        await db.execute('INSERT OR REPLACE INTO alliance_members (alliance_id, user_id, role) VALUES (?, ?, ?)', (aid, ctx.author.id, "owner"))
        await db.commit()
    await ctx.send(f"🏰 Alliance **{name}** created.")

@alliance.command(name="join", description="Join an alliance")
async def alliance_join(ctx: commands.Context, name: str):
    cfg = await _cfg_get(ctx.guild.id, ["alliances_enabled"])
    if cfg.get("alliances_enabled", 1) == 0:
        return await ctx.send("⚠️ Alliances are disabled for this server.")
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT alliance_id FROM alliances WHERE guild_id = ? AND name = ?', (ctx.guild.id, name)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("Alliance not found.")
        aid = row[0]
        await db.execute('INSERT OR IGNORE INTO alliance_members (alliance_id, user_id, role) VALUES (?, ?, ?)', (aid, ctx.author.id, "member"))
        await db.commit()
    await ctx.send(f"🤝 You joined **{name}**.")

@alliance.command(name="info", description="View alliance info")
async def alliance_info(ctx: commands.Context, name: str):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT alliance_id, bank, owner_id FROM alliances WHERE guild_id = ? AND name = ?', (ctx.guild.id, name)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("Alliance not found.")
        aid, bank, owner_id = row
        async with db.execute('SELECT user_id, role FROM alliance_members WHERE alliance_id = ?', (aid,)) as c2:
            members = await c2.fetchall()
    owner = ctx.guild.get_member(owner_id)
    owner_name = owner.display_name if owner else f"User({owner_id})"
    mtext = "\n".join(f"- {ctx.guild.get_member(uid).mention if ctx.guild.get_member(uid) else uid} ({role})" for uid, role in members) or "No members."
    embed = discord.Embed(title=f"🏰 Alliance: {name}", color=discord.Color.gold(), timestamp=discord.utils.utcnow())
    embed.add_field(name="Owner", value=owner_name)
    embed.add_field(name="Bank", value=f"{bank:,} coins")
    embed.add_field(name="Members", value=mtext, inline=False)
    await ctx.send(embed=embed)

@alliance.command(name="deposit", description="Deposit coins into alliance bank")
async def alliance_deposit(ctx: commands.Context, name: str, amount: int):
    cfg = await _cfg_get(ctx.guild.id, ["alliances_enabled"])
    if cfg.get("alliances_enabled", 1) == 0:
        return await ctx.send("⚠️ Alliances are disabled for this server.")
    if amount <= 0:
        return await ctx.send("Enter a positive amount.")
    data = await get_global_money(ctx.author.id)
    if data['balance'] < amount:
        return await ctx.send("You don't have enough coins.")
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT alliance_id FROM alliances WHERE guild_id = ? AND name = ?', (ctx.guild.id, name)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("Alliance not found.")
        aid = row[0]
        await db.execute('UPDATE alliances SET bank = bank + ? WHERE alliance_id = ?', (amount, aid))
        await db.commit()
    await update_global_balance(ctx.author.id, -amount)
    await ctx.send(f"🏦 Deposited **{amount:,}** into **{name}**.")

@alliance.command(name="withdraw", description="Owner withdraws coins from alliance bank")
async def alliance_withdraw(ctx: commands.Context, name: str, amount: int):
    cfg = await _cfg_get(ctx.guild.id, ["alliances_enabled"])
    if cfg.get("alliances_enabled", 1) == 0:
        return await ctx.send("⚠️ Alliances are disabled for this server.")
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT alliance_id, bank, owner_id FROM alliances WHERE guild_id = ? AND name = ?', (ctx.guild.id, name)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("Alliance not found.")
        aid, bank, owner_id = row
        if ctx.author.id != owner_id:
            return await ctx.send("Only the owner can withdraw.")
        if amount <= 0 or amount > bank:
            return await ctx.send("Invalid amount.")
        await db.execute('UPDATE alliances SET bank = bank - ? WHERE alliance_id = ?', (amount, aid))
        await db.commit()
    await update_global_balance(ctx.author.id, amount)
    await ctx.send(f"🏦 Withdrew **{amount:,}** from **{name}**.")

@bot.hybrid_group(name="market", description="Player marketplace")
async def market(ctx: commands.Context):
    if ctx.interaction:
        await ctx.interaction.response.defer(ephemeral=False)
    else:
        await ctx.send("Use a subcommand.")

@market.command(name="list", description="List an item for sale")
async def market_list(ctx: commands.Context, item: str, price: int, quantity: int = 1):
    item = item.strip()
    if not item or price <= 0 or quantity <= 0:
        return await ctx.send("Provide a valid item, positive price, and quantity.")
    if item not in LOOT_ITEMS:
        return await ctx.send("You can only list boss loot items. Use their IDs (e.g., epic_sword).")
    cfg = await _cfg_get(ctx.guild.id, ["marketplace_enabled"])
    if cfg.get("marketplace_enabled", 1) == 0:
        return await ctx.send("🛒 Marketplace is disabled for this server.")
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT INTO market_listings (guild_id, seller_id, item, price, quantity, created_at) VALUES (?, ?, ?, ?, ?, ?)', (ctx.guild.id, ctx.author.id, item, price, quantity, int(time.time())))
        await db.commit()
    await ctx.send(f"🛒 Listed **{item}** for **{price:,}** (x{quantity}).")

@market.command(name="view", description="View current listings")
async def market_view(ctx: commands.Context):
    cfg = await _cfg_get(ctx.guild.id, ["marketplace_enabled"])
    if cfg.get("marketplace_enabled", 1) == 0:
        return await ctx.send("🛒 Marketplace is disabled for this server.")
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT listing_id, seller_id, item, price, quantity FROM market_listings WHERE guild_id = ? ORDER BY created_at DESC LIMIT 10', (ctx.guild.id,)) as c:
            rows = await c.fetchall()
    if not rows:
        return await ctx.send("No listings.")
    lines = []
    for lid, sid, item, price, qty in rows:
        seller = ctx.guild.get_member(sid)
        sname = seller.display_name if seller else f"User({sid})"
        lines.append(f"#{lid} • {item} • {price:,} • x{qty} • by {sname}")
    embed = discord.Embed(title="🛒 Marketplace Listings", description="\n".join(lines), color=discord.Color.green(), timestamp=discord.utils.utcnow())
    await ctx.send(embed=embed)

@market.command(name="buy", description="Buy from a listing")
async def market_buy(ctx: commands.Context, listing_id: int, quantity: int = 1):
    if quantity <= 0:
        return await ctx.send("Quantity must be positive.")
    cfg = await _cfg_get(ctx.guild.id, ["marketplace_enabled","marketplace_tax"])
    if cfg.get("marketplace_enabled", 1) == 0:
        return await ctx.send("🛒 Marketplace is disabled for this server.")
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute('SELECT seller_id, item, price, quantity FROM market_listings WHERE listing_id = ? AND guild_id = ?', (listing_id, ctx.guild.id)) as c:
            row = await c.fetchone()
        if not row:
            return await ctx.send("Listing not found.")
        seller_id, item, price, avail_qty = row
        if item not in LOOT_ITEMS:
            return await ctx.send("This listing contains an invalid item.")
        if ctx.author.id == seller_id:
            return await ctx.send("You cannot buy your own listing.")
        if quantity > avail_qty:
            return await ctx.send("Not enough quantity available.")
        total = price * quantity
        buyer = await get_global_money(ctx.author.id)
        if buyer['balance'] < total:
            return await ctx.send("You don't have enough coins.")
        tax_pct = int(cfg.get("marketplace_tax", 0) or 0)
        tax_amt = int(total * (tax_pct / 100.0)) if tax_pct > 0 else 0
        seller_take = total - tax_amt
        await update_global_balance(ctx.author.id, -total)
        await update_global_balance(seller_id, seller_take)
        await db.execute('INSERT INTO boss_items (user_id, item_id, count) VALUES (?, ?, ?) ON CONFLICT(user_id, item_id) DO UPDATE SET count = count + ?', (ctx.author.id, item, quantity, quantity))
        new_qty = avail_qty - quantity
        if new_qty == 0:
            await db.execute('DELETE FROM market_listings WHERE listing_id = ?', (listing_id,))
        else:
            await db.execute('UPDATE market_listings SET quantity = ? WHERE listing_id = ?', (new_qty, listing_id))
        await db.commit()
    msg = f"✅ Bought **{quantity}x {item}** for **{total:,}**."
    if tax_amt > 0:
        msg += f" Tax: **{tax_amt:,}**."
    await ctx.send(msg)

@bot.hybrid_group(name="vassal", description="Vassal sponsorships")
async def vassal(ctx: commands.Context):
    if ctx.interaction:
        await ctx.interaction.response.defer(ephemeral=False)
    else:
        await ctx.send("Use a subcommand.")
    cfg = await _cfg_get(ctx.guild.id, ["alliances_enabled"])
    if cfg.get("alliances_enabled", 1) == 0:
        await ctx.send("⚠️ Vassals are disabled for this server.")

@vassal.command(name="sponsor", description="Sponsor a vassal")
@app_commands.describe(member="User to sponsor", percent="Contribution percent (max 15)")
async def vassal_sponsor(ctx: commands.Context, member: discord.Member, percent: int = 5):
    cfg = await _cfg_get(ctx.guild.id, ["alliances_enabled","vassal_max_percent"])
    if cfg.get("alliances_enabled", 1) == 0:
        return await ctx.send("⚠️ Vassals are disabled for this server.")
    maxp = int(cfg.get("vassal_max_percent", 15) or 15)
    if percent < 1 or percent > maxp:
        return await ctx.send(f"Percent must be between 1 and {maxp}.")
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR REPLACE INTO vassals (lord_id, vassal_id, guild_id, percent) VALUES (?, ?, ?, ?)', (ctx.author.id, member.id, ctx.guild.id, percent))
        await db.commit()
    await ctx.send(f"🤝 {member.mention} is now your vassal at {percent}% contribution.")

@vassal.command(name="remove", description="Remove vassal sponsorship")
async def vassal_remove(ctx: commands.Context, member: discord.Member):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('DELETE FROM vassals WHERE lord_id = ? AND vassal_id = ? AND guild_id = ?', (ctx.author.id, member.id, ctx.guild.id))
        await db.commit()
    await ctx.send("🔚 Sponsorship removed.")

@bot.hybrid_command(name="ping", description="Check the bot's latency")
async def ping(ctx: commands.Context):
    latency = round(bot.latency * 1000)
    embed = discord.Embed(title="🏓 Pong!", description=f"Latency: **{latency}ms**", color=0x00ff00)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="membercount", description="Display server member statistics")
async def membercount(ctx: commands.Context):
    guild = ctx.guild
    total = guild.member_count
    bots = sum(1 for m in guild.members if m.bot)
    humans = total - bots
    online = sum(1 for m in guild.members if m.status != discord.Status.offline)

    embed = discord.Embed(title=f"📈 {guild.name} Member Count", color=0x00d2ff)
    embed.add_field(name="Total Members", value=f"👥 `{total}`", inline=True)
    embed.add_field(name="Humans", value=f"👤 `{humans}`", inline=True)
    embed.add_field(name="Bots", value=f"🤖 `{bots}`", inline=True)
    embed.add_field(name="Online", value=f"🟢 `{online}`", inline=True)
    embed.set_thumbnail(url=guild.icon.url if guild.icon else None)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="serverinfo", description="Show detailed server information")
async def serverinfo(ctx: commands.Context):
    guild = ctx.guild
    owner = guild.owner
    created_at = guild.created_at.strftime("%b %d, %Y")
    roles = len(guild.roles)
    channels = len(guild.channels)
    emojis = len(guild.emojis)
    boosts = guild.premium_subscription_count
    level = guild.premium_tier

    embed = discord.Embed(title=f"🏰 {guild.name} Information", color=0x00d2ff)
    embed.set_thumbnail(url=guild.icon.url if guild.icon else None)
    
    embed.add_field(name="Owner", value=f"👑 {owner.mention}", inline=True)
    embed.add_field(name="Created On", value=f"📅 {created_at}", inline=True)
    embed.add_field(name="Server ID", value=f"🆔 `{guild.id}`", inline=True)
    
    embed.add_field(name="Members", value=f"👥 `{guild.member_count}`", inline=True)
    embed.add_field(name="Channels", value=f"📁 `{channels}`", inline=True)
    embed.add_field(name="Roles", value=f"🎭 `{roles}`", inline=True)
    
    embed.add_field(name="Boosts", value=f"💎 `{boosts}` (Level {level})", inline=True)
    embed.add_field(name="Emojis", value=f"😀 `{emojis}`", inline=True)
    embed.add_field(name="Verification", value=f"🛡️ {guild.verification_level.name.title()}", inline=True)

    if guild.banner:
        embed.set_image(url=guild.banner.url)

    await ctx.send(embed=embed)

@bot.hybrid_command(name="userinfo", description="Show detailed information about a user")
async def userinfo(ctx: commands.Context, member: discord.Member = None):
    target = member or ctx.author
    joined_at = target.joined_at.strftime("%b %d, %Y")
    created_at = target.created_at.strftime("%b %d, %Y")
    roles = [role.mention for role in target.roles[1:]] # Skip @everyone
    
    embed = discord.Embed(title=f"👤 User Information: {target.display_name}", color=target.color)
    embed.set_thumbnail(url=target.display_avatar.url)
    
    embed.add_field(name="Username", value=f"`{target.name}`", inline=True)
    embed.add_field(name="ID", value=f"`{target.id}`", inline=True)
    embed.add_field(name="Status", value=f"{target.status.name.title()}", inline=True)
    
    embed.add_field(name="Joined Server", value=f"📥 {joined_at}", inline=True)
    embed.add_field(name="Joined Discord", value=f"📅 {created_at}", inline=True)
    embed.add_field(name="Bot?", value=f"{'Yes' if target.bot else 'No'}", inline=True)
    
    if roles:
        embed.add_field(name=f"Roles [{len(roles)}]", value=" ".join(roles[:10]) + ("..." if len(roles) > 10 else ""), inline=False)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="avatar", description="Display a user's avatar")
async def avatar(ctx: commands.Context, member: discord.Member = None):
    target = member or ctx.author
    embed = discord.Embed(title=f"🖼️ Avatar of {target.display_name}", color=0x00d2ff)
    embed.set_image(url=target.display_avatar.url)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="help_nexus", description="List all commands or get help for a specific category")
async def help_cmd_new(ctx: commands.Context, category: str = None):
    # Dynamic categories based on command tags/groups
    categories = {
        "Economy": ["balance", "deposit", "withdraw", "work", "crime", "rob", "shop", "buy", "profile", "leaderboard", "jobs", "applyjob", "autodeposit", "vote"],
        "Moderation": ["modsystem", "mods", "mod profile", "mod lb", "kick", "ban", "warn", "warnings", "clearwarns", "automod", "reportabuse", "resolveabuse"],
        "Security": ["raided start", "raided stop", "raidmode", "antiphish"],
        "Utility": ["ping", "membercount", "serverinfo", "userinfo", "avatar", "setup", "setprefix", "autoaddrole"],
        "Admin": ["sync", "syncall", "diagnose", "servers", "analytics"]
    }

    if not category:
        embed = discord.Embed(
            title="📚 Empire Nexus Help",
            description="Welcome to the Empire! Use `/help <category>` for more details on a specific section.",
            color=0x00d2ff
        )
        embed.set_thumbnail(url=bot.user.display_avatar.url)
        
        for cat, cmds in categories.items():
            embed.add_field(name=f"🔹 {cat}", value=f"`{len(cmds)} commands`", inline=True)
            
        embed.set_footer(text="Join our support server for more help! /setup for the link.")
        prefix = await get_prefix(bot, ctx.message)
        owner_ok = (ctx.guild and (ctx.author.id == ctx.guild.owner_id)) or (ctx.guild and await has_owner_access(ctx.guild.id, ctx.author.id))
        view = HelpView(prefix, ctx.author.id, owner_ok)
        return await ctx.send(embed=embed, view=view)

    cat_name = category.capitalize()
    if cat_name not in categories:
        return await ctx.send(f"❌ Category `{category}` not found! Use `/help` to see all categories.")

    embed = discord.Embed(title=f"📖 {cat_name} Commands", color=0x00d2ff)
    cmd_list = categories[cat_name]
    
    for cmd_name in cmd_list:
        cmd = bot.get_command(cmd_name)
        if cmd:
            desc = cmd.description or "No description provided."
            usage = f"/{cmd.qualified_name} {cmd.signature}"
            embed.add_field(name=f"/{cmd.qualified_name}", value=f"{desc}\n`Usage: {usage}`", inline=False)

    await ctx.send(embed=embed)

# --- Admin Commands ---


@bot.hybrid_command(name="addxp", description="[OWNER ONLY] Add XP to a user")
@is_authorized_owner()
async def add_xp_admin(ctx: commands.Context, member: discord.Member, amount: int):
    if amount <= 0:
        return await ctx.send("Amount must be positive.")
    
    # Confirmation prompt
    await ctx.send(f"⚠️ Are you sure you want to add **{amount:,} XP** to {member.mention}? (Type `confirm` to proceed)")
    
    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() == "confirm"
    
    try:
        await bot.wait_for('message', check=check, timeout=30)
    except:
        return await ctx.send("Operation cancelled.")

    leveled_up, new_level = await add_xp(member.id, ctx.guild.id, amount)
    
    msg = f"✅ Added **{amount:,} XP** to {member.mention}."
    if leveled_up:
        msg += f"\n🎊 They leveled up to **Level {new_level}**!"
    
    await ctx.send(msg)

@bot.hybrid_command(name="addtitle", description="[OWNER ONLY] Add a custom title to a user")
@is_authorized_owner()
async def add_title_admin(ctx: commands.Context, member: discord.Member, title: str):
    # Confirmation prompt
    await ctx.send(f"⚠️ Are you sure you want to add the title '**{title}**' to {member.mention}? (Type `confirm` to proceed)")
    
    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() == "confirm"
    
    try:
        await bot.wait_for('message', check=check, timeout=30)
    except:
        return await ctx.send("Operation cancelled.")

    await ensure_rewards(member.id)
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT titles_json FROM user_rewards WHERE user_id = ?", (member.id,)) as cursor:
            row = await cursor.fetchone()
            titles = json.loads(row[0]) if row else []
        
        titles.append({"title": title, "source": "admin", "timestamp": int(time.time())})
        
        await db.execute("UPDATE user_rewards SET titles_json = ? WHERE user_id = ?", (json.dumps(titles), member.id))
        await db.commit()
    
    await ctx.send(f"✅ Added title '**{title}**' as a permanent badge for {member.mention}.")

@bot.hybrid_command(name="setprefix", description="Change the bot's prefix for this server")
@owner_or_admin()
async def set_prefix_cmd(ctx: commands.Context, new_prefix: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            INSERT INTO guild_config (guild_id, prefix) VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET prefix = excluded.prefix
        ''', (ctx.guild.id, new_prefix))
        await db.commit()
    try:
        PREFIX_CACHE[ctx.guild.id] = new_prefix
    except:
        pass
    await ctx.send(f"✅ Prefix successfully updated to `{new_prefix}`")

@bot.hybrid_command(name="addowner", description="Grant owner-command access to a user")
@is_guild_owner_only()
async def add_owner_cmd(ctx: commands.Context, member: discord.Member):
    if member.id == ctx.guild.owner_id:
        return await ctx.send("They are already the server owner.")
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('INSERT OR IGNORE INTO owner_access (guild_id, user_id) VALUES (?, ?)', (ctx.guild.id, member.id))
        await db.commit()
    await ctx.send(f"✅ {member.mention} can now use owner-only commands.")

@bot.hybrid_command(name="removeowner", description="Revoke owner-command access from a user")
@is_guild_owner_only()
async def remove_owner_cmd(ctx: commands.Context, member: discord.Member):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('DELETE FROM owner_access WHERE guild_id = ? AND user_id = ?', (ctx.guild.id, member.id))
        await db.commit()
    await ctx.send(f"✅ {member.mention} can no longer use owner-only commands.")
def _parse_duration(s: str) -> int:
    s = str(s).strip().lower()
    try:
        if s.endswith("m"):
            return int(s[:-1]) * 60
        if s.endswith("h"):
            return int(s[:-1]) * 3600
        if s.endswith("d"):
            return int(s[:-1]) * 86400
        return int(s) * 60
    except:
        return 0
@bot.hybrid_group(name="raided", description="Raid response")
@owner_or_admin()
async def raided(ctx: commands.Context):
    if ctx.interaction:
        await ctx.interaction.response.defer(ephemeral=False)
    else:
        await ctx.send("Use a subcommand.")

@raided.command(name="start", description="Start raid lockdown, purge, timeout, and ban spammers")
async def raided_start(ctx: commands.Context, duration: str, restoretime: str):
    if not ctx.guild:
        return
    dur = _parse_duration(duration)
    rst = _parse_duration(restoretime)
    if dur <= 0 or rst <= 0:
        return await ctx.send("Enter valid duration and restoretime like 30m and 1h.")
    threshold_dt = datetime.datetime.utcnow() - datetime.timedelta(seconds=rst)
    deleted_total = 0
    spam_counts = {}
    try:
        invites = await ctx.guild.invites()
        for inv in invites:
            try:
                await inv.delete()
            except:
                pass
    except:
        pass
    try:
        dr = ctx.guild.default_role
        perms = dr.permissions
        perms.update(create_instant_invite=False)
        await dr.edit(permissions=perms)
    except:
        pass
    snapshot = {}
    try:
        for ch in list(ctx.guild.text_channels) + list(ctx.guild.voice_channels):
            try:
                ov = ch.overwrites_for(ctx.guild.default_role)
                snapshot[str(ch.id)] = {
                    "view_channel": ov.view_channel,
                    "send_messages": getattr(ov, "send_messages", None),
                    "connect": getattr(ov, "connect", None)
                }
                await ch.set_permissions(ctx.guild.default_role, view_channel=False, send_messages=False, connect=False)
            except:
                pass
    except:
        pass
    timeout_list = []
    until = discord.utils.utcnow() + datetime.timedelta(seconds=dur)
    try:
        for m in ctx.guild.members:
            if not m.bot:
                try:
                    await m.timeout(until, reason="Raid lockdown")
                    timeout_list.append(m.id)
                except:
                    pass
    except:
        pass
    try:
        for ch in ctx.guild.text_channels:
            try:
                msgs = await ch.purge(after=threshold_dt, bulk=True)
                for msg in msgs or []:
                    if not getattr(msg.author, "bot", False):
                        spam_counts[msg.author.id] = spam_counts.get(msg.author.id, 0) + 1
                deleted_total += len(msgs or [])
            except:
                pass
    except:
        pass
    try:
        for uid, cnt in spam_counts.items():
            if cnt >= 10:
                try:
                    u = ctx.guild.get_member(uid)
                    if u:
                        await ctx.guild.ban(u, reason="Spam during raid window", delete_message_days=1)
                except:
                    pass
    except:
        pass
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR REPLACE INTO raid_state (guild_id, started_at, duration_sec, restore_sec, lock_active, channel_overwrites_json, timeouts_json) VALUES (?, ?, ?, ?, ?, ?, ?)', 
                             (ctx.guild.id, int(time.time()), dur, rst, 1, json.dumps(snapshot), json.dumps(timeout_list)))
            await db.commit()
    except:
        pass
    await ctx.send(f"✅ Raid started. Deleted ~{deleted_total} messages, locked channels, applied timeouts, and banned spammers.")

@raided.command(name="stop", description="Stop raid lockdown and restore channel access")
async def raided_stop(ctx: commands.Context):
    if not ctx.guild:
        return
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute('SELECT channel_overwrites_json, timeouts_json FROM raid_state WHERE guild_id = ?', (ctx.guild.id,)) as c:
                row = await c.fetchone()
            if not row:
                return await ctx.send("No raid state found.")
            snapshot = json.loads(row[0] or "{}")
            timeout_list = json.loads(row[1] or "[]")
    except:
        snapshot = {}
        timeout_list = []
    try:
        dr = ctx.guild.default_role
        perms = dr.permissions
        perms.update(create_instant_invite=True)
        await dr.edit(permissions=perms)
    except:
        pass
    try:
        for ch in list(ctx.guild.text_channels) + list(ctx.guild.voice_channels):
            data = snapshot.get(str(ch.id))
            if data is not None:
                try:
                    await ch.set_permissions(ctx.guild.default_role, 
                                             view_channel=data.get("view_channel"), 
                                             send_messages=data.get("send_messages"), 
                                             connect=data.get("connect"))
                except:
                    pass
    except:
        pass
    try:
        for uid in timeout_list:
            m = ctx.guild.get_member(uid)
            if m:
                try:
                    await m.timeout(None, reason="Raid stop")
                except:
                    pass
    except:
        pass
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('UPDATE raid_state SET lock_active = 0 WHERE guild_id = ?', (ctx.guild.id,))
            await db.commit()
    except:
        pass
    await ctx.send("✅ Raid stopped. Restored channel access and lifted timeouts.")

@bot.hybrid_command(name="autoaddrole", description="Auto-assign a role to new members; optional mass add")
@owner_or_admin()
@app_commands.describe(role="Role to auto-assign", mass_add="Also assign to current members")
async def autoaddrole(ctx: commands.Context, role: discord.Role, mass_add: bool = False):
    if not ctx.guild:
        return
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('INSERT OR REPLACE INTO guild_auto_role (guild_id, role_id) VALUES (?, ?)', (ctx.guild.id, role.id))
            await db.commit()
    except:
        return await ctx.send("Failed to save auto role.")
    assigned = 0
    if mass_add:
        for m in ctx.guild.members:
            if not m.bot and role not in m.roles:
                try:
                    await m.add_roles(role, reason="Mass auto-assign")
                    assigned += 1
                except:
                    pass
    await ctx.send(f"✅ Auto role set to {role.mention}.{' Assigned to ' + str(assigned) + ' members.' if mass_add else ''}")
if __name__ == '__main__':
    bot.run(TOKEN)
