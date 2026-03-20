import asyncio
import logging
import os
import socket
from datetime import datetime, timedelta, time, date
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    ForeignKey,
    select,
    and_,
    or_,
    func,
    text,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session, selectinload


logging.basicConfig(level=logging.INFO)

API_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not API_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is required.")
ADMIN_ID = 1652603985

DATABASE_URL = "sqlite:///booking_bot.db"

# Минимум за сколько до начала слота можно записаться / перенести (новое время)
BOOKING_MIN_ADVANCE = timedelta(hours=1)
MASSAGE_MIN_ADVANCE = timedelta(minutes=30)
SESSION_DURATION = timedelta(hours=1)
MASSAGE_BUFFER_AFTER_ANY_SESSION = timedelta(minutes=30)

SERVICE_TRAINING = "training"
SERVICE_MASSAGE = "massage"

# Часовой пояс расписания (слоты и «сейчас» для записи)
APP_TZ = ZoneInfo("Asia/Yekaterinburg")
POLLING_RETRY_SECONDS = 5


def now_naive_local() -> datetime:
    """Текущее локальное время без tzinfo (как в БД для слотов)."""
    return datetime.now(APP_TZ).replace(tzinfo=None, microsecond=0)


def today_local() -> date:
    return datetime.now(APP_TZ).date()


engine = create_engine(DATABASE_URL, echo=False, future=True)
Base = declarative_base()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    tg_id = Column(Integer, unique=True, index=True, nullable=False)
    username = Column(String, nullable=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    is_admin = Column(Boolean, default=False)
    package_total = Column(Integer, default=0)
    package_remaining = Column(Integer, default=0)
    massage_package_total = Column(Integer, default=0)
    massage_package_remaining = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    trainings = relationship("Training", back_populates="user")


class Training(Base):
    __tablename__ = "trainings"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    service_type = Column(String, default=SERVICE_TRAINING)
    start_at = Column(DateTime, nullable=False)
    status = Column(String, default="scheduled")  # scheduled, cancelled, completed, missed
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    reminder_client_sent = Column(Boolean, default=False)
    reminder_admin_sent = Column(Boolean, default=False)
    canceled_by_admin = Column(Boolean, default=False)
    # Уведомление админу «прошла ли тренировка?» уже отправлено
    post_session_prompt_sent = Column(Boolean, default=False)

    user = relationship("User", back_populates="trainings")


class BlockedSlot(Base):
    __tablename__ = "blocked_slots"

    id = Column(Integer, primary_key=True, index=True)
    start_at = Column(DateTime, nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(bind=engine)


def ensure_db_columns():
    """Добавить колонки в существующую SQLite-БД (create_all не меняет старые таблицы)."""
    with engine.begin() as conn:
        user_rows = conn.execute(text("PRAGMA table_info(users)")).fetchall()
        user_col_names = {r[1] for r in user_rows}
        if "massage_package_total" not in user_col_names:
            conn.execute(
                text(
                    "ALTER TABLE users ADD COLUMN massage_package_total INTEGER DEFAULT 0"
                )
            )
        if "massage_package_remaining" not in user_col_names:
            conn.execute(
                text(
                    "ALTER TABLE users ADD COLUMN massage_package_remaining INTEGER DEFAULT 0"
                )
            )

        rows = conn.execute(text("PRAGMA table_info(trainings)")).fetchall()
        col_names = {r[1] for r in rows}
        if "service_type" not in col_names:
            conn.execute(
                text(
                    f"ALTER TABLE trainings ADD COLUMN service_type VARCHAR DEFAULT '{SERVICE_TRAINING}'"
                )
            )
        if "post_session_prompt_sent" not in col_names:
            conn.execute(
                text(
                    "ALTER TABLE trainings ADD COLUMN post_session_prompt_sent BOOLEAN DEFAULT 0"
                )
            )


ensure_db_columns()


def wipe_database():
    """Полная очистка таблиц (продакшн-сброс)."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM trainings"))
        conn.execute(text("DELETE FROM users"))


def sync_all_admin_flags():
    """Только один админ — ADMIN_ID; у остальных is_admin=False."""
    with get_session() as session:
        users = session.scalars(select(User)).all()
        for u in users:
            u.is_admin = u.tg_id == ADMIN_ID
        session.commit()


def get_session() -> Session:
    return SessionLocal()


def main_menu_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="Тренировки")],
        [KeyboardButton(text="Массаж")],
        [KeyboardButton(text="Мой пакет")],
    ]
    if is_admin:
        buttons.append([KeyboardButton(text="🛠 Админ панель")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def trainings_menu_kb() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="📝 Записаться на тренировку")],
        [KeyboardButton(text="❌ Отменить/перенести запись")],
        [KeyboardButton(text="📋 Мои записи")],
        [KeyboardButton(text="📦 Мой пакет тренировок")],
        [KeyboardButton(text="⬅️ Назад в меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def massage_menu_kb() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="💆 Записаться на массаж")],
        [KeyboardButton(text="❌ Отменить/перенести массаж")],
        [KeyboardButton(text="📋 Мои массажи")],
        [KeyboardButton(text="📦 Мой пакет массажа")],
        [KeyboardButton(text="⬅️ Назад в меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_menu_kb() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="👥 Клиенты")],
        [KeyboardButton(text="📆 Все записи (Тренировки)")],
        [KeyboardButton(text="📆 Все записи (Массаж)")],
        [KeyboardButton(text="🔒 Забронировать время")],
        [KeyboardButton(text="⬅️ Назад в меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


MONTH_NAMES = (
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
)

WEEKDAYS_SHORT_RU = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")

MY_BOOK_ROOT = "mybook:root"
MY_BOOK_ALL = "mybook:all"
MY_MASS_ROOT = "mymass:root"
MY_MASS_ALL = "mymass:all"

# Админ: «Все записи» — тот же UX, что «Мои записи» у клиента
ADM_ALL_ROOT = "admtrbook:root"
ADM_ALL_ALL = "admtrbook:all"
ADM_MASS_ROOT = "admmass:root"
ADM_MASS_ALL = "admmass:all"
ADM_BLOCK_ROOT = "admblock:root"
ADM_BLOCK_ALL = "admblock:all"


def admin_all_bookings_root_text(has_any: bool, service_type: str = SERVICE_TRAINING) -> str:
    title = "Все текущие записи (Массаж):" if service_type == SERVICE_MASSAGE else "Все текущие записи (Тренировки):"
    lines = [
        title,
        "",
        "Выбери день (неделя вперёд) или нажми «Показать все записи».",
    ]
    if not has_any:
        lines.extend(["", "<i>Активных записей пока нет.</i>"])
    return "\n".join(lines)


def build_admin_all_bookings_keyboard(service_type: str = SERVICE_TRAINING) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    today = today_local()
    for i in range(7):
        d = today + timedelta(days=i)
        wd = WEEKDAYS_SHORT_RU[d.weekday()]
        builder.button(
            text=f"{wd} {d.strftime('%d.%m')}",
            callback_data=(
                f"admmassday:{d.isoformat()}"
                if service_type == SERVICE_MASSAGE
                else f"admtrbookday:{d.isoformat()}"
            ),
        )
    builder.adjust(3)
    builder.row(
        InlineKeyboardButton(
            text="📋 Показать все записи",
            callback_data=ADM_MASS_ALL if service_type == SERVICE_MASSAGE else ADM_ALL_ALL,
        )
    )
    return builder.as_markup()


def admin_block_root_text(has_any: bool) -> str:
    lines = [
        "🔒 Забронированное время (блокировки):",
        "",
        "Выбери день (неделя вперёд) или нажми «Показать все блокировки».",
        "Заблокированный час скрывается из записи и для тренировок, и для массажа.",
    ]
    if not has_any:
        lines.extend(["", "<i>Пока нет заблокированных слотов.</i>"])
    return "\n".join(lines)


def build_admin_block_root_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    today = today_local()
    for i in range(7):
        d = today + timedelta(days=i)
        wd = WEEKDAYS_SHORT_RU[d.weekday()]
        builder.button(
            text=f"{wd} {d.strftime('%d.%m')}",
            callback_data=f"admblockday:{d.isoformat()}",
        )
    builder.adjust(3)
    builder.row(
        InlineKeyboardButton(
            text="📋 Показать все блокировки",
            callback_data=ADM_BLOCK_ALL,
        )
    )
    return builder.as_markup()


def build_admin_block_day_keyboard(
    selected_date: date,
    blocked_starts: List[datetime],
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    blocked_set = {dt for dt in blocked_starts}
    dt = datetime.combine(selected_date, time(8, 0))
    end_dt = datetime.combine(selected_date, time(19, 0))
    now_dt = now_naive_local()
    while dt <= end_dt:
        if dt > now_dt and dt not in blocked_set:
            builder.button(
                text=dt.strftime("%H:%M"),
                callback_data=f"admblocktime:{dt.isoformat()}",
            )
        dt += timedelta(hours=1)
    builder.adjust(4)
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data=ADM_BLOCK_ROOT))
    return builder.as_markup()


def calendar_title(current_date: date) -> str:
    """Заголовок для календаря: месяц и год."""
    return f"{MONTH_NAMES[current_date.month - 1]} {current_date.year}"


def generate_calendar_keyboard(current_date: date) -> InlineKeyboardMarkup:
    today = today_local()
    end_date = today + timedelta(days=14)

    builder = InlineKeyboardBuilder()

    month_start = date(current_date.year, current_date.month, 1)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    month_end = next_month - timedelta(days=1)

    d = month_start
    while d <= month_end and d <= end_date:
        if d >= today:
            builder.button(
                text=d.strftime("%d.%m"),
                callback_data=f"date:{d.isoformat()}",
            )
        d += timedelta(days=1)

    builder.adjust(4)

    nav_row = []
    if month_start > today.replace(day=1):
        prev_month = (month_start - timedelta(days=1)).replace(day=1)
        nav_row.append(
            InlineKeyboardButton(text="⬅️", callback_data=f"cal:{prev_month.isoformat()}")
        )
    if month_end < end_date:
        nav_row.append(
            InlineKeyboardButton(text="➡️", callback_data=f"cal:{next_month.isoformat()}")
        )
    if nav_row:
        builder.row(*nav_row)

    builder.row(
        InlineKeyboardButton(text="Отмена", callback_data="cancel_booking_flow")
    )

    return builder.as_markup()


def earliest_bookable_moment(service_type: str) -> datetime:
    if service_type == SERVICE_MASSAGE:
        return now_naive_local() + MASSAGE_MIN_ADVANCE
    return now_naive_local() + BOOKING_MIN_ADVANCE


def is_slot_available_for_service(
    candidate_start: datetime,
    existing_slots: List[datetime],
    service_type: str,
) -> bool:
    candidate_end = candidate_start + SESSION_DURATION
    for existing_start in existing_slots:
        existing_end = existing_start + SESSION_DURATION
        overlaps = candidate_start < existing_end and candidate_end > existing_start
        if overlaps:
            return False
        # Для массажа нужен буфер 30 минут после любого уже стоящего сеанса.
        if (
            service_type == SERVICE_MASSAGE
            and existing_end <= candidate_start
            and candidate_start < existing_end + MASSAGE_BUFFER_AFTER_ANY_SESSION
        ):
            return False
    return True


def get_busy_slots_for_date(
    session: Session,
    selected_date: date,
    exclude_training_id: Optional[int] = None,
) -> List[datetime]:
    trainings_stmt = select(Training.start_at).where(
        and_(
            Training.status == "scheduled",
            func.date(Training.start_at) == selected_date,
        )
    )
    if exclude_training_id is not None:
        trainings_stmt = trainings_stmt.where(Training.id != exclude_training_id)
    training_slots = [row[0] for row in session.execute(trainings_stmt).all()]

    blocked_stmt = (
        select(BlockedSlot.start_at)
        .where(func.date(BlockedSlot.start_at) == selected_date)
        .order_by(BlockedSlot.start_at)
    )
    blocked_slots = [row[0] for row in session.execute(blocked_stmt).all()]
    return training_slots + blocked_slots


def generate_time_keyboard(
    selected_date: date,
    existing_slots: List[datetime],
    service_type: str = SERVICE_TRAINING,
    skip_min_advance: bool = False,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    start_time = time(8, 0)
    end_time = time(19, 0)

    current_datetime = now_naive_local()
    min_slot_start = earliest_bookable_moment(service_type)

    dt = datetime.combine(selected_date, start_time)
    while dt.time() <= end_time:
        # Не показывать уже прошедшие слоты в выбранный календарный день (по Asia/Yekaterinburg)
        if dt <= current_datetime:
            dt += timedelta(minutes=15)
            continue

        # Клиент: минимум за час до начала; перенос админом — без этого ограничения
        if not skip_min_advance and dt < min_slot_start:
            dt += timedelta(minutes=15)
            continue

        if is_slot_available_for_service(dt, existing_slots, service_type):
            builder.button(
                text=dt.strftime("%H:%M"),
                callback_data=f"time:{dt.isoformat()}",
            )

        dt += timedelta(minutes=15)

    builder.adjust(4)
    builder.row(
        InlineKeyboardButton(text="Назад к датам", callback_data="back_to_dates"),
        InlineKeyboardButton(text="Отмена", callback_data="cancel_booking_flow"),
    )
    return builder.as_markup()


user_states: Dict[int, Dict[str, str]] = {}

# Человекочитаемые статусы тренировок
STATUS_LABELS = {
    "scheduled": "запланирована",
    "cancelled": "отменена",
    "completed": "проведена",
    "missed": "пропущена",
}


def status_label(status: str) -> str:
    return STATUS_LABELS.get(status, status)


def is_channel_admin(tg_user_id: int) -> bool:
    return tg_user_id == ADMIN_ID


async def reject_unless_admin(callback: CallbackQuery) -> bool:
    """True если можно продолжать (пользователь — админ)."""
    if not is_channel_admin(callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return False
    return True


async def ensure_user(message: Message) -> User:
    with get_session() as session:
        stmt = select(User).where(User.tg_id == message.from_user.id)
        user = session.scalar(stmt)
        if not user:
            user = User(
                tg_id=message.from_user.id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
                is_admin=is_channel_admin(message.from_user.id),
            )
            session.add(user)
        else:
            user.username = message.from_user.username
            user.first_name = message.from_user.first_name
            user.last_name = message.from_user.last_name
        # Единственный админ в системе — по ADMIN_ID
        user.is_admin = is_channel_admin(user.tg_id)
        session.commit()
        session.refresh(user)
        return user


async def update_phone(user_tg_id: int, phone: str):
    with get_session() as session:
        stmt = select(User).where(User.tg_id == user_tg_id)
        user = session.scalar(stmt)
        if user:
            user.phone = phone
            session.commit()


async def cmd_start(message: Message):
    user = await ensure_user(message)

    if not user.phone:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Отправить телефон", request_contact=True)]
            ],
            resize_keyboard=True,
        )
        await message.answer(
            "Привет! Для регистрации отправь, пожалуйста, свой номер телефона кнопкой ниже.",
            reply_markup=kb,
        )
    else:
        await message.answer(
            "Рад тебя видеть! Выбирай действие в меню ниже.",
            reply_markup=main_menu_kb(is_admin=user.is_admin),
        )


async def handle_contact(message: Message):
    if not message.contact or not message.contact.phone_number:
        return

    await update_phone(message.from_user.id, message.contact.phone_number)

    with get_session() as session:
        stmt = select(User).where(User.tg_id == message.from_user.id)
        user = session.scalar(stmt)
        if user:
            user.is_admin = is_channel_admin(user.tg_id)
            session.commit()
            session.refresh(user)

    await message.answer(
        "Регистрация завершена! Теперь ты можешь записываться на тренировки.",
        reply_markup=main_menu_kb(is_admin=bool(user and user.is_admin)),
    )


async def handle_main_menu(message: Message):
    user = await ensure_user(message)
    text = message.text or ""

    # Пока нет телефона — не показываем основное меню, просим контакт
    if not user.phone:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📱 Отправить телефон", request_contact=True)]
            ],
            resize_keyboard=True,
        )
        await message.answer(
            "Для доступа к тренировкам нужна регистрация: отправь свой номер телефона кнопкой ниже.",
            reply_markup=kb,
        )
        return

    if text == "Тренировки":
        await message.answer(
            "Здесь можно записаться на тренировку, отменить или перенести запись, посмотреть свои записи.",
            reply_markup=trainings_menu_kb(),
        )
    elif text == "Массаж":
        await message.answer(
            "Здесь можно записаться на массаж, отменить или перенести запись, посмотреть свои записи.",
            reply_markup=massage_menu_kb(),
        )
    elif text == "Мой пакет":
        await send_my_package(message, user)
    elif text == "🛠 Админ панель" and user.is_admin and is_channel_admin(
        message.from_user.id
    ):
        await message.answer("Админ панель:", reply_markup=admin_menu_kb())
    elif text == "⬅️ Назад в меню":
        await message.answer(
            "Главное меню:",
            reply_markup=main_menu_kb(is_admin=user.is_admin),
        )
    elif text == "📝 Записаться на тренировку":
        await start_booking_flow(message, user, SERVICE_TRAINING)
    elif text == "💆 Записаться на массаж":
        await start_booking_flow(message, user, SERVICE_MASSAGE)
    elif text == "❌ Отменить/перенести запись":
        await start_cancel_reschedule_flow(message, user, SERVICE_TRAINING)
    elif text == "❌ Отменить/перенести массаж":
        await start_cancel_reschedule_flow(message, user, SERVICE_MASSAGE)
    elif text == "📋 Мои записи":
        await send_my_bookings(message, user, SERVICE_TRAINING)
    elif text == "📋 Мои массажи":
        await send_my_bookings(message, user, SERVICE_MASSAGE)
    elif text == "📦 Мой пакет тренировок":
        await send_service_package(message, user, SERVICE_TRAINING)
    elif text == "📦 Мой пакет массажа":
        await send_service_package(message, user, SERVICE_MASSAGE)
    elif text == "👥 Клиенты" and user.is_admin and is_channel_admin(
        message.from_user.id
    ):
        await admin_show_clients(message, page=0)
    elif text == "📆 Все записи (Тренировки)" and user.is_admin and is_channel_admin(
        message.from_user.id
    ):
        await admin_show_all_trainings(message, SERVICE_TRAINING)
    elif text == "📆 Все записи (Массаж)" and user.is_admin and is_channel_admin(
        message.from_user.id
    ):
        await admin_show_all_trainings(message, SERVICE_MASSAGE)
    elif text == "🔒 Забронировать время" and user.is_admin and is_channel_admin(
        message.from_user.id
    ):
        await admin_show_blocked_slots(message)
    else:
        await message.answer(
            "Не понял команду. Используй кнопки меню ниже.",
            reply_markup=main_menu_kb(is_admin=user.is_admin),
        )


async def send_my_package(message: Message, user: User):
    text = (
        "📦 Твои пакеты\n\n"
        f"Тренировки: всего {user.package_total}, осталось {user.package_remaining}\n"
        f"Массаж: всего {user.massage_package_total}, осталось {user.massage_package_remaining}"
    )
    await message.answer(text, reply_markup=main_menu_kb(is_admin=user.is_admin))


def service_label(service_type: str) -> str:
    return "массаж" if service_type == SERVICE_MASSAGE else "тренировку"


def service_plural(service_type: str) -> str:
    return "массажей" if service_type == SERVICE_MASSAGE else "тренировок"


async def send_service_package(message: Message, user: User, service_type: str):
    if service_type == SERVICE_MASSAGE:
        total = user.massage_package_total
        remaining = user.massage_package_remaining
        title = "💆 Твой пакет массажа"
        menu_section = "«Массаж»"
    else:
        total = user.package_total
        remaining = user.package_remaining
        title = "📦 Твой пакет тренировок"
        menu_section = "«Тренировки»"

    text = (
        f"{title}\n\n"
        f"Всего: {total}\n"
        f"Осталось: {remaining}"
    )
    if remaining > 0:
        text += f"\n\nЗаписаться можно в разделе {menu_section}."
    else:
        text += "\n\nЧтобы записаться, попроси тренера пополнить пакет."
    await message.answer(
        text,
        reply_markup=main_menu_kb(is_admin=user.is_admin),
    )


async def start_booking_flow(message: Message, user: User, service_type: str):
    remaining = (
        user.massage_package_remaining
        if service_type == SERVICE_MASSAGE
        else user.package_remaining
    )
    if remaining <= 0:
        await message.answer(
            f"У тебя нет доступных {service_plural(service_type)}. Обратись к тренеру, чтобы пополнить пакет.",
            reply_markup=main_menu_kb(is_admin=user.is_admin),
        )
        return

    user_states[message.from_user.id] = {"flow": "booking", "service_type": service_type}

    today = today_local()
    min_advance_text = "30 минут" if service_type == SERVICE_MASSAGE else "1 час"
    await message.answer(
        f"📅 {calendar_title(today)}\n\n"
        f"Выбери дату для записи на {service_label(service_type)}.\n"
        f"⏱ Запись только не позднее чем за <b>{min_advance_text}</b> до начала слота.",
        reply_markup=generate_calendar_keyboard(today),
    )


def my_bookings_root_text(has_upcoming: bool) -> str:
    lines = [
        "Мои текущие записи:",
        "",
        "Выбери день (на неделю вперёд) или нажми «Показать все записи».",
    ]
    if not has_upcoming:
        lines.extend(["", "<i>Сейчас нет предстоящих записей.</i>"])
    return "\n".join(lines)


def build_my_bookings_root_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    today = today_local()
    for i in range(7):
        d = today + timedelta(days=i)
        wd = WEEKDAYS_SHORT_RU[d.weekday()]
        builder.button(
            text=f"{wd} {d.strftime('%d.%m')}",
            callback_data=f"mybookday:{d.isoformat()}",
        )
    builder.adjust(3)
    builder.row(
        InlineKeyboardButton(
            text="📋 Показать все записи",
            callback_data=MY_BOOK_ALL,
        )
    )
    return builder.as_markup()


def build_my_massage_root_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    today = today_local()
    for i in range(7):
        d = today + timedelta(days=i)
        wd = WEEKDAYS_SHORT_RU[d.weekday()]
        builder.button(
            text=f"{wd} {d.strftime('%d.%m')}",
            callback_data=f"mymassday:{d.isoformat()}",
        )
    builder.adjust(3)
    builder.row(
        InlineKeyboardButton(
            text="📋 Показать все записи",
            callback_data=MY_MASS_ALL,
        )
    )
    return builder.as_markup()


async def start_cancel_reschedule_flow(message: Message, user: User, service_type: str):
    with get_session() as session:
        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == user.id,
                    Training.service_type == service_type,
                    Training.status == "scheduled",
                    Training.start_at >= now_naive_local(),
                )
            )
            .order_by(Training.start_at)
        )
        trainings = session.scalars(stmt).all()

    if not trainings:
        await message.answer(
            "У тебя нет активных записей для отмены или переноса.",
            reply_markup=massage_menu_kb() if service_type == SERVICE_MASSAGE else trainings_menu_kb(),
        )
        return

    builder = InlineKeyboardBuilder()
    for t in trainings:
        text = t.start_at.strftime("%d.%m %H:%M")
        builder.button(
            text=text,
            callback_data=f"edit_my:{t.id}",
        )
    builder.adjust(1)

    await message.answer(
        f"Выбери {service_label(service_type)} для отмены или переноса:",
        reply_markup=builder.as_markup(),
    )


async def send_my_bookings(message: Message, user: User, service_type: str = SERVICE_TRAINING):
    now = now_naive_local()
    with get_session() as session:
        cnt = session.scalar(
            select(func.count(Training.id)).where(
                and_(
                    Training.user_id == user.id,
                    Training.service_type == service_type,
                    Training.status == "scheduled",
                    Training.start_at >= now,
                )
            )
        )
        has_upcoming = (cnt or 0) > 0

    await message.answer(
        my_bookings_root_text(has_upcoming) if service_type == SERVICE_TRAINING else "Мои текущие массажи:\n\nВыбери день (на неделю вперёд) или нажми «Показать все записи».",
        reply_markup=build_my_bookings_root_keyboard() if service_type == SERVICE_TRAINING else build_my_massage_root_keyboard(),
    )


async def cb_my_bookings_root(callback: CallbackQuery):
    """Назад к списку дней."""
    with get_session() as session:
        u = session.scalar(select(User).where(User.tg_id == callback.from_user.id))
        if not u:
            await callback.answer("Сначала пройди регистрацию.", show_alert=True)
            return
        now = now_naive_local()
        cnt = session.scalar(
            select(func.count(Training.id)).where(
                and_(
                    Training.user_id == u.id,
                    Training.service_type == SERVICE_TRAINING,
                    Training.status == "scheduled",
                    Training.start_at >= now,
                )
            )
        )
        has_upcoming = (cnt or 0) > 0

    await callback.message.edit_text(
        my_bookings_root_text(has_upcoming),
        reply_markup=build_my_bookings_root_keyboard(),
    )
    await callback.answer()


async def cb_my_bookings_day(callback: CallbackQuery):
    """Записи на выбранный день."""
    if not callback.data or not callback.data.startswith("mybookday:"):
        await callback.answer()
        return
    _, iso = callback.data.split(":", 1)
    try:
        target_date = date.fromisoformat(iso)
    except ValueError:
        await callback.answer("Неверная дата.", show_alert=True)
        return

    with get_session() as session:
        u = session.scalar(select(User).where(User.tg_id == callback.from_user.id))
        if not u:
            await callback.answer("Сначала пройди регистрацию.", show_alert=True)
            return
        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == u.id,
                    Training.service_type == SERVICE_TRAINING,
                    Training.status == "scheduled",
                    func.date(Training.start_at) == target_date,
                )
            )
            .order_by(Training.start_at)
        )
        day_trainings = session.scalars(stmt).all()

    if not day_trainings:
        body = (
            f"Записи на <b>{target_date.strftime('%d.%m.%Y')}</b>:\n\n"
            f"На этот день нет записей."
        )
    else:
        lines = [
            f"Записи на <b>{target_date.strftime('%d.%m.%Y')}</b>:",
            "",
        ]
        for t in day_trainings:
            lines.append(
                f"• {t.start_at.strftime('%H:%M')} — {status_label(t.status)}"
            )
        body = "\n".join(lines)

    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=MY_BOOK_ROOT)]
        ]
    )
    await callback.message.edit_text(body, reply_markup=back_kb)
    await callback.answer()


async def cb_my_bookings_all(callback: CallbackQuery):
    """Все предстоящие записи."""
    now = now_naive_local()
    with get_session() as session:
        u = session.scalar(select(User).where(User.tg_id == callback.from_user.id))
        if not u:
            await callback.answer("Сначала пройди регистрацию.", show_alert=True)
            return
        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == u.id,
                    Training.service_type == SERVICE_TRAINING,
                    Training.status == "scheduled",
                    Training.start_at >= now,
                )
            )
            .order_by(Training.start_at)
        )
        trainings = session.scalars(stmt).all()

    if not trainings:
        body = "Все предстоящие записи:\n\nНет активных записей."
    else:
        lines = ["Все предстоящие записи:", ""]
        for t in trainings:
            lines.append(
                f"• {t.start_at.strftime('%d.%m %H:%M')} — {status_label(t.status)}"
            )
        body = "\n".join(lines)

    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=MY_BOOK_ROOT)]
        ]
    )
    await callback.message.edit_text(body, reply_markup=back_kb)
    await callback.answer()


async def cb_my_massage_root(callback: CallbackQuery):
    with get_session() as session:
        u = session.scalar(select(User).where(User.tg_id == callback.from_user.id))
        if not u:
            await callback.answer("Сначала пройди регистрацию.", show_alert=True)
            return
        now = now_naive_local()
        cnt = session.scalar(
            select(func.count(Training.id)).where(
                and_(
                    Training.user_id == u.id,
                    Training.service_type == SERVICE_MASSAGE,
                    Training.status == "scheduled",
                    Training.start_at >= now,
                )
            )
        )
        has_upcoming = (cnt or 0) > 0

    body = "Мои текущие массажи:\n\nВыбери день (на неделю вперёд) или нажми «Показать все записи»."
    if not has_upcoming:
        body += "\n\n<i>Сейчас нет предстоящих записей.</i>"
    await callback.message.edit_text(body, reply_markup=build_my_massage_root_keyboard())
    await callback.answer()


async def cb_my_massage_day(callback: CallbackQuery):
    if not callback.data or not callback.data.startswith("mymassday:"):
        await callback.answer()
        return
    _, iso = callback.data.split(":", 1)
    try:
        target_date = date.fromisoformat(iso)
    except ValueError:
        await callback.answer("Неверная дата.", show_alert=True)
        return

    with get_session() as session:
        u = session.scalar(select(User).where(User.tg_id == callback.from_user.id))
        if not u:
            await callback.answer("Сначала пройди регистрацию.", show_alert=True)
            return
        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == u.id,
                    Training.service_type == SERVICE_MASSAGE,
                    Training.status == "scheduled",
                    func.date(Training.start_at) == target_date,
                )
            )
            .order_by(Training.start_at)
        )
        items = session.scalars(stmt).all()

    if not items:
        body = f"Массажи на <b>{target_date.strftime('%d.%m.%Y')}</b>:\n\nНа этот день нет записей."
    else:
        lines = [f"Массажи на <b>{target_date.strftime('%d.%m.%Y')}</b>:", ""]
        for t in items:
            lines.append(f"• {t.start_at.strftime('%H:%M')} — {status_label(t.status)}")
        body = "\n".join(lines)

    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=MY_MASS_ROOT)]]
    )
    await callback.message.edit_text(body, reply_markup=back_kb)
    await callback.answer()


async def cb_my_massage_all(callback: CallbackQuery):
    now = now_naive_local()
    with get_session() as session:
        u = session.scalar(select(User).where(User.tg_id == callback.from_user.id))
        if not u:
            await callback.answer("Сначала пройди регистрацию.", show_alert=True)
            return
        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == u.id,
                    Training.service_type == SERVICE_MASSAGE,
                    Training.status == "scheduled",
                    Training.start_at >= now,
                )
            )
            .order_by(Training.start_at)
        )
        items = session.scalars(stmt).all()

    if not items:
        body = "Все предстоящие массажи:\n\nНет активных записей."
    else:
        lines = ["Все предстоящие массажи:", ""]
        for t in items:
            lines.append(f"• {t.start_at.strftime('%d.%m %H:%M')} — {status_label(t.status)}")
        body = "\n".join(lines)

    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=MY_MASS_ROOT)]]
    )
    await callback.message.edit_text(body, reply_markup=back_kb)
    await callback.answer()


async def cb_calendar_navigation(callback: CallbackQuery):
    if not callback.data:
        return
    _, date_str = callback.data.split(":", 1)
    target_date = date.fromisoformat(date_str)
    st = user_states.get(callback.from_user.id, {})
    service_type = st.get("service_type", SERVICE_TRAINING)
    service_name = "массажа" if service_type == SERVICE_MASSAGE else "тренировки"
    await callback.message.edit_text(
        f"📅 {calendar_title(target_date)}\n\nВыбери дату для {service_name}:",
        reply_markup=generate_calendar_keyboard(target_date),
    )
    await callback.answer()


async def cb_cancel_booking_flow(callback: CallbackQuery):
    st = user_states.pop(callback.from_user.id, None) or {}
    service_type = st.get("service_type", SERVICE_TRAINING)
    await callback.message.edit_text("Запись отменена.")
    try:
        await callback.bot.send_message(
            chat_id=callback.from_user.id,
            text="Выбери действие в меню:",
            reply_markup=massage_menu_kb() if service_type == SERVICE_MASSAGE else trainings_menu_kb(),
        )
    except Exception:
        pass
    await callback.answer()


async def cb_back_to_dates(callback: CallbackQuery):
    today = today_local()
    st = user_states.get(callback.from_user.id, {})
    service_type = st.get("service_type", SERVICE_TRAINING)
    service_name = "массажа" if service_type == SERVICE_MASSAGE else "тренировки"
    await callback.message.edit_text(
        f"📅 {calendar_title(today)}\n\nВыбери дату для {service_name}:",
        reply_markup=generate_calendar_keyboard(today),
    )
    await callback.answer()


async def cb_select_date(callback: CallbackQuery):
    if not callback.data:
        return
    _, date_str = callback.data.split(":", 1)
    selected_date = date.fromisoformat(date_str)

    st = user_states.get(callback.from_user.id, {})
    skip_min = st.get("flow") == "admin_reschedule"
    service_type = st.get("service_type", SERVICE_TRAINING)

    with get_session() as session:
        existing_slots = get_busy_slots_for_date(session, selected_date)

    await callback.message.edit_text(
        f"Дата {selected_date.strftime('%d.%m')}. Выбери время:",
        reply_markup=generate_time_keyboard(
            selected_date, existing_slots, service_type=service_type, skip_min_advance=skip_min
        ),
    )

    await callback.answer()


async def cb_select_time(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not callback.data:
        return
    _, dt_str = callback.data.split(":", 1)
    selected_dt = datetime.fromisoformat(dt_str)

    state = user_states.get(user_id, {})
    flow = state.get("flow")
    service_type = state.get("service_type", SERVICE_TRAINING)

    min_dt = earliest_bookable_moment(service_type)
    # Перенос тренером — без ограничения «за час» (срочные правки расписания)
    if flow in ("booking", "client_reschedule"):
        if selected_dt < min_dt:
            await callback.answer(
                "Это время недоступно по ограничению минимального времени записи. Выбери другое.",
                show_alert=True,
            )
            return

    with get_session() as session:
        user = session.scalar(select(User).where(User.tg_id == user_id))
        if not user:
            await callback.answer("Пользователь не найден.", show_alert=True)
            return

        if flow == "booking":
            existing_slots = get_busy_slots_for_date(session, selected_dt.date())
            if not is_slot_available_for_service(selected_dt, existing_slots, service_type):
                await callback.answer(
                    "Это время уже занято. Выбери другое.", show_alert=True
                )
                return

            remaining = (
                user.massage_package_remaining
                if service_type == SERVICE_MASSAGE
                else user.package_remaining
            )
            if remaining <= 0:
                await callback.answer(
                    "У тебя нет доступных сеансов. Обратись к тренеру.", show_alert=True
                )
                return

            t = Training(
                user_id=user.id,
                service_type=service_type,
                start_at=selected_dt,
                status="scheduled",
            )
            if service_type == SERVICE_MASSAGE:
                user.massage_package_remaining -= 1
            else:
                user.package_remaining -= 1
            session.add(t)
            session.commit()
            session.refresh(t)

            rest = (
                user.massage_package_remaining
                if service_type == SERVICE_MASSAGE
                else user.package_remaining
            )
            service_noun = "массаж" if service_type == SERVICE_MASSAGE else "тренировку"
            await callback.message.edit_text(
                f"✅ Ты записан на {service_noun} {selected_dt.strftime('%d.%m %H:%M')}.\n"
                f"Осталось в пакете: {rest}",
            )
            try:
                await callback.bot.send_message(
                    chat_id=user_id,
                    text="Можешь записаться ещё или посмотреть «Мои записи».",
                    reply_markup=massage_menu_kb() if service_type == SERVICE_MASSAGE else trainings_menu_kb(),
                )
            except Exception:
                pass

            await callback.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"Новая запись ({'Массаж' if service_type == SERVICE_MASSAGE else 'Тренировка'}):\n"
                    f"Клиент: @{user.username or user.first_name}\n"
                    f"Время: {t.start_at.strftime('%d.%m %H:%M')}"
                ),
            )
            user_states.pop(user_id, None)
            await callback.answer()
        elif flow == "client_reschedule":
            training_id = state.get("training_id")
            if not training_id:
                await callback.answer("Ошибка состояния переноса.", show_alert=True)
                return

            t = session.get(Training, int(training_id))
            if not t or t.status != "scheduled":
                await callback.answer("Запись не найдена или уже изменена.", show_alert=True)
                return
            service_type = t.service_type or SERVICE_TRAINING

            existing_slots = get_busy_slots_for_date(
                session,
                selected_dt.date(),
                exclude_training_id=t.id,
            )
            if not is_slot_available_for_service(selected_dt, existing_slots, service_type):
                await callback.answer(
                    "Это время уже занято. Выбери другое.", show_alert=True
                )
                return

            old_time = t.start_at
            t.start_at = selected_dt
            t.post_session_prompt_sent = False
            t.reminder_client_sent = False
            t.reminder_admin_sent = False
            session.commit()

            await callback.message.edit_text(
                f"✅ Запись перенесена с {old_time.strftime('%d.%m %H:%M')} "
                f"на {t.start_at.strftime('%d.%m %H:%M')}."
            )
            try:
                await callback.bot.send_message(
                    chat_id=user.tg_id,
                    text=(
                        f"Ты перенёс свою тренировку.\n"
                        if service_type == SERVICE_TRAINING
                        else "Ты перенёс свой массаж.\n"
                    )
                    + (
                        f"Новое время: {t.start_at.strftime('%d.%m %H:%M')}"
                    ),
                    reply_markup=massage_menu_kb() if service_type == SERVICE_MASSAGE else trainings_menu_kb(),
                )
            except Exception:
                pass
            await callback.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"Клиент @{user.username or user.first_name} перенёс "
                    f"{'массаж' if service_type == SERVICE_MASSAGE else 'тренировку'}.\n"
                    f"Новое время: {t.start_at.strftime('%d.%m %H:%M')}"
                ),
            )
            user_states.pop(user_id, None)
            await callback.answer()
        elif flow == "admin_reschedule":
            if not is_channel_admin(user_id):
                await callback.answer("Нет доступа.", show_alert=True)
                return
            training_id = state.get("training_id")
            if not training_id:
                await callback.answer("Ошибка состояния переноса.", show_alert=True)
                return

            t = session.scalar(
                select(Training)
                .options(selectinload(Training.user))
                .where(
                    and_(
                        Training.id == int(training_id),
                        Training.status == "scheduled",
                    )
                )
            )
            if not t:
                await callback.answer("Тренировка не найдена или уже изменена.", show_alert=True)
                return
            service_type = t.service_type or SERVICE_TRAINING

            existing_slots = get_busy_slots_for_date(
                session,
                selected_dt.date(),
                exclude_training_id=t.id,
            )
            if not is_slot_available_for_service(selected_dt, existing_slots, service_type):
                await callback.answer(
                    "Это время уже занято. Выбери другое.", show_alert=True
                )
                return

            old_time = t.start_at
            client_tg = t.user.tg_id
            client_uname = t.user.username or t.user.first_name or "клиент"
            t.start_at = selected_dt
            t.post_session_prompt_sent = False
            t.reminder_client_sent = False
            t.reminder_admin_sent = False
            session.commit()

            new_when = selected_dt.strftime("%d.%m %H:%M")
            old_when = old_time.strftime("%d.%m %H:%M")

            await callback.message.edit_text(
                f"Запись перенесена с {old_when} на {new_when}."
            )

            await callback.bot.send_message(
                chat_id=client_tg,
                text=(
                    f"Твоя тренировка была перенесена тренером.\n"
                    if service_type == SERVICE_TRAINING
                    else "Твой массаж был перенесен тренером.\n"
                )
                + (
                    f"Новое время: {new_when}"
                ),
            )

            await callback.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"Ты перенёс тренировку клиента @{client_uname}\n"
                    if service_type == SERVICE_TRAINING
                    else f"Ты перенёс массаж клиента @{client_uname}\n"
                )
                + (
                    f"Новое время: {new_when}"
                ),
            )

            user_states.pop(user_id, None)
            await callback.answer()
        else:
            await callback.answer("Неизвестный сценарий выбора времени.", show_alert=True)


async def cb_edit_my(callback: CallbackQuery):
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)

    with get_session() as session:
        t = session.get(Training, training_id)
        if not t or t.status != "scheduled":
            await callback.answer("Запись не найдена или уже изменена.", show_alert=True)
            return

        dt = t.start_at

    builder = InlineKeyboardBuilder()
    builder.button(text="Отменить", callback_data=f"cancel_my:{training_id}")
    builder.button(text="Перенести", callback_data=f"reschedule_my:{training_id}")
    builder.adjust(2)

    await callback.message.edit_text(
        f"Запись на {dt.strftime('%d.%m %H:%M')}. Что сделать?",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


async def cb_cancel_my(callback: CallbackQuery):
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)
    user_tg_id = callback.from_user.id

    now = now_naive_local()

    with get_session() as session:
        t = session.get(Training, training_id)
        if not t or t.status != "scheduled":
            await callback.answer("Запись не найдена или уже изменена.", show_alert=True)
            return

        user = session.scalar(select(User).where(User.id == t.user_id))

        delta = t.start_at - now
        refundable = delta >= timedelta(hours=4)

        t.status = "cancelled"
        if refundable:
            if (t.service_type or SERVICE_TRAINING) == SERVICE_MASSAGE:
                user.massage_package_remaining += 1
            else:
                user.package_remaining += 1

        session.commit()

        await callback.message.edit_text(
            f"❌ Запись на {t.start_at.strftime('%d.%m %H:%M')} отменена.\n"
            + ("Сеанс вернулся в твой пакет." if refundable else "Меньше чем за 4 часа — сеанс сгорает."),
        )
        try:
            await callback.bot.send_message(
                chat_id=user_tg_id,
                text="Можешь записаться на другое время или посмотреть «Мои записи».",
                reply_markup=trainings_menu_kb(),
            )
        except Exception:
            pass
        await callback.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"Клиент @{user.username or user.first_name} отменил тренировку "
                f"{t.start_at.strftime('%d.%m %H:%M')}.\n"
                f"{'Возврат в пакет.' if refundable else 'Тренировка сгорела.'}"
            ),
        )

    await callback.answer()


async def cb_reschedule_my(callback: CallbackQuery):
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)
    user_id = callback.from_user.id

    with get_session() as session:
        t = session.get(Training, training_id)
        if not t or t.status != "scheduled":
            await callback.answer("Запись не найдена или уже изменена.", show_alert=True)
            return

        delta = t.start_at - now_naive_local()
        if delta < timedelta(hours=4):
            await callback.answer(
                "Перенести можно не позднее чем за 4 часа до тренировки.", show_alert=True
            )
            return

    user_states[user_id] = {
        "flow": "client_reschedule",
        "training_id": str(training_id),
        "service_type": t.service_type or SERVICE_TRAINING,
    }

    today = today_local()
    await callback.message.edit_text(
        f"📅 {calendar_title(today)}\n\nВыбери новую дату для переноса тренировки:",
        reply_markup=generate_calendar_keyboard(today),
    )
    await callback.answer()


async def admin_show_clients(message: Message, page: int = 0):
    page_size = 5
    offset = page * page_size

    with get_session() as session:
        total = session.scalar(select(func.count(User.id)))
        stmt = (
            select(User)
            .order_by(User.created_at.desc())
            .offset(offset)
            .limit(page_size)
        )
        clients = session.scalars(stmt).all()

    if not clients:
        await message.answer("Клиенты ещё не зарегистрированы.")
        return

    text_lines = [f"Страница {page + 1}"]
    builder = InlineKeyboardBuilder()
    for c in clients:
        label = (
            f"{c.first_name or ''} {c.last_name or ''}".strip()
            or f"@{c.username}" if c.username else f"id{c.tg_id}"
        )
        label += f" | тр: {c.package_remaining}, мс: {c.massage_package_remaining}"
        text_lines.append(label)
        builder.button(
            text=label,
            callback_data=f"client:{c.id}",
        )
    builder.adjust(1)

    nav_buttons = []
    if offset > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text="⬅️ Назад", callback_data=f"clients_page:{page - 1}"
            )
        )
    if offset + page_size < total:
        nav_buttons.append(
            InlineKeyboardButton(
                text="Вперёд ➡️", callback_data=f"clients_page:{page + 1}"
            )
        )
    if nav_buttons:
        builder.row(*nav_buttons)

    await message.answer(
        "Клиенты (карточки):",
        reply_markup=builder.as_markup(),
    )


async def cb_clients_page(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data:
        return
    _, page_str = callback.data.split(":", 1)
    page = int(page_str)
    await callback.message.delete()
    await admin_show_clients(callback.message, page=page)
    await callback.answer()


async def cb_client_card(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data:
        return
    _, client_id_str = callback.data.split(":", 1)
    client_id = int(client_id_str)

    with get_session() as session:
        client = session.get(User, client_id)
        if not client:
            await callback.answer("Клиент не найден.", show_alert=True)
            return

        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == client.id,
                    Training.status == "scheduled",
                )
            )
            .order_by(Training.start_at)
        )
        trainings = session.scalars(stmt).all()

    if client.username:
        client_line = f"👤 Клиент: @{client.username}"
    else:
        nm = f"{client.first_name or ''} {client.last_name or ''}".strip() or "без имени"
        client_line = f"👤 Клиент: {nm} (нет @username, tg id: {client.tg_id})"
    caption_lines = [
        client_line,
        f"📱 Телефон: {client.phone or 'не указан'}",
        f"📦 Пакет: всего {client.package_total}, осталось {client.package_remaining}",
        f"💆 Массаж: всего {client.massage_package_total}, осталось {client.massage_package_remaining}",
        "",
        "Предстоящие и ожидающие итога:",
    ]
    if trainings:
        now = now_naive_local()
        for t in trainings[:10]:
            if t.start_at < now:
                caption_lines.append(
                    f"• {t.start_at.strftime('%d.%m %H:%M')} — ⏳ ждёт твоего ответа (прошла ли)"
                )
            else:
                caption_lines.append(
                    f"• {t.start_at.strftime('%d.%m %H:%M')} — {status_label(t.status)}"
                )
    else:
        caption_lines.append("— нет активных записей")

    builder = InlineKeyboardBuilder()
    builder.button(
        text="Задать пакет", callback_data=f"setpkg:{client.id}"
    )
    builder.button(
        text="Задать пакет (массаж)", callback_data=f"setpkgmass:{client.id}"
    )
    builder.button(
        text="Записи клиента", callback_data=f"client_tr:{client.id}"
    )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(text="Закрыть", callback_data="close_msg")
    )

    await callback.message.edit_text(
        "\n".join(line for line in caption_lines if line),
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


admin_states: Dict[int, Dict[str, str]] = {}


async def cb_set_package(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data:
        return
    _, client_id_str = callback.data.split(":", 1)
    client_id = int(client_id_str)

    admin_states[callback.from_user.id] = {
        "action": "set_package",
        "client_id": str(client_id),
    }

    await callback.message.edit_text(
        "Введи количество тренировок для клиента (целое число).\nОтменить: напиши «отмена»."
    )
    await callback.answer()


async def cb_set_massage_package(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data:
        return
    _, client_id_str = callback.data.split(":", 1)
    client_id = int(client_id_str)

    admin_states[callback.from_user.id] = {
        "action": "set_massage_package",
        "client_id": str(client_id),
    }

    await callback.message.edit_text(
        "Введи количество сеансов массажа для клиента (целое число).\nОтменить: напиши «отмена»."
    )
    await callback.answer()


async def handle_admin_text(message: Message):
    if not is_channel_admin(message.from_user.id):
        return False
    state = admin_states.get(message.from_user.id)
    if not state:
        return False

    if state.get("action") in ("set_package", "set_massage_package"):
        client_id = int(state["client_id"])
        if message.text.strip().lower() in ("отмена", "отменить", "cancel"):
            admin_states.pop(message.from_user.id, None)
            await message.answer("Действие отменено.", reply_markup=admin_menu_kb())
            return True
        try:
            value = int(message.text.strip())
            if value < 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно ввести неотрицательное целое число (или напиши «отмена»).")
            return True

        with get_session() as session:
            client = session.get(User, client_id)
            if not client:
                await message.answer("Клиент не найден.")
                admin_states.pop(message.from_user.id, None)
                return True

            is_massage = state.get("action") == "set_massage_package"
            if is_massage:
                diff = value - client.massage_package_total
                client.massage_package_total = value
                client.massage_package_remaining = max(client.massage_package_remaining + diff, 0)
                pkg_rem = client.massage_package_remaining
            else:
                diff = value - client.package_total
                client.package_total = value
                client.package_remaining = max(client.package_remaining + diff, 0)
                pkg_rem = client.package_remaining
            session.commit()
            session.refresh(client)
            c_username = client.username
            c_first = client.first_name

        admin_states.pop(message.from_user.id, None)
        if c_username:
            who = f"@{c_username}"
        else:
            who = c_first or "клиент (без @username)"
        await message.answer(
            f"Клиенту {who} задано <b>{value}</b> "
            f"{'сеансов массажа' if state.get('action') == 'set_massage_package' else 'тренировок'}.\n"
            f"В пакете осталось: {pkg_rem}.",
            reply_markup=admin_menu_kb(),
        )
        return True

    return False


async def cb_client_trainings(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data:
        return
    _, client_id_str = callback.data.split(":", 1)
    client_id = int(client_id_str)

    with get_session() as session:
        client = session.get(User, client_id)
        if not client:
            await callback.answer("Клиент не найден.", show_alert=True)
            return

        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == client.id,
                    Training.status == "scheduled",
                )
            )
            .order_by(Training.start_at)
        )
        trainings = session.scalars(stmt).all()

    if not trainings:
        await callback.message.edit_text("У клиента нет активных записей.")
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    text_lines = ["Записи клиента:"]
    for t in trainings:
        text_lines.append(
            f"{t.start_at.strftime('%d.%m %H:%M')} — {status_label(t.status)}"
        )
        builder.button(
            text=f"{t.start_at.strftime('%d.%m %H:%M')}",
            callback_data=f"admtr:{t.id}",
        )
    builder.adjust(1)
    builder.row(
        InlineKeyboardButton(text="Назад к клиенту", callback_data=f"client:{client.id}")
    )

    await callback.message.edit_text(
        "\n".join(text_lines),
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


async def cb_admin_training(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)

    with get_session() as session:
        t = session.scalar(
            select(Training)
            .options(selectinload(Training.user))
            .where(Training.id == training_id)
        )
        if not t:
            await callback.answer("Тренировка не найдена.", show_alert=True)
            return

        client = t.user
        cid = client.id
        tun = client.username or client.first_name or "клиент"
        tid = t.id
        tst = t.start_at.strftime("%d.%m %H:%M")
        st_lab = status_label(t.status)
        is_scheduled = t.status == "scheduled"

    builder = InlineKeyboardBuilder()
    if is_scheduled:
        builder.button(
            text="Отменить (вернуть/сжечь по правилу)",
            callback_data=f"admcancel:{training_id}",
        )
        builder.button(
            text="Перенести",
            callback_data=f"admresch:{training_id}",
        )
        builder.adjust(1)
    builder.row(
        InlineKeyboardButton(text="Назад", callback_data=f"client_tr:{cid}")
    )

    extra = "" if is_scheduled else "\n\n(Только просмотр — запись уже не активна.)"
    await callback.message.edit_text(
        f"Тренировка ID {tid}\n"
        f"Клиент: @{tun}\n"
        f"Время: {tst}\n"
        f"Статус: {st_lab}"
        + extra,
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


async def cb_admin_cancel_training(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)

    now = now_naive_local()

    with get_session() as session:
        t = session.get(Training, training_id)
        if not t or t.status != "scheduled":
            await callback.answer("Тренировка не найдена или уже изменена.", show_alert=True)
            return

        client = t.user
        delta = t.start_at - now
        refundable = delta >= timedelta(hours=4)

        t.status = "cancelled"
        t.canceled_by_admin = True
        if refundable:
            if (t.service_type or SERVICE_TRAINING) == SERVICE_MASSAGE:
                client.massage_package_remaining += 1
            else:
                client.package_remaining += 1

        session.commit()

        # Все поля до закрытия сессии (иначе DetachedInstanceError после commit)
        client_tg_id = client.tg_id
        client_uname = client.username or client.first_name or "клиент"
        when_str = t.start_at.strftime("%d.%m %H:%M")

    await callback.message.edit_text(
        f"Тренировка клиента @{client_uname} "
        f"на {when_str} отменена тренером.\n"
        + ("Возврат в пакет." if refundable else "Меньше чем за 4 часа, тренировка сгорает."),
    )

    await callback.bot.send_message(
        chat_id=client_tg_id,
        text=(
            f"Твоя тренировка {when_str} была отменена тренером.\n"
            f"{'Тренировка вернулась в твой пакет.' if refundable else 'Меньше чем за 4 часа, тренировка сгорела.'}"
        ),
    )

    await callback.answer()


async def cb_admin_reschedule_training(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)
    admin_id = callback.from_user.id

    with get_session() as session:
        t = session.get(Training, training_id)
        service_type = (t.service_type if t else SERVICE_TRAINING) or SERVICE_TRAINING

    user_states[admin_id] = {
        "flow": "admin_reschedule",
        "training_id": str(training_id),
        "service_type": service_type,
    }

    today = today_local()
    await callback.message.edit_text(
        f"📅 {calendar_title(today)}\n\nВыбери новую дату для тренировки клиента:",
        reply_markup=generate_calendar_keyboard(today),
    )
    await callback.answer()


async def cb_close_msg(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()


async def admin_show_all_trainings(message: Message, service_type: str = SERVICE_TRAINING):
    with get_session() as session:
        cnt = session.scalar(
            select(func.count(Training.id)).where(
                and_(
                    Training.status == "scheduled",
                    Training.service_type == service_type,
                )
            )
        )
        has_any = (cnt or 0) > 0

    await message.answer(
        admin_all_bookings_root_text(has_any, service_type),
        reply_markup=build_admin_all_bookings_keyboard(service_type),
    )


async def admin_show_blocked_slots(message: Message):
    now = now_naive_local()
    with get_session() as session:
        cnt = session.scalar(
            select(func.count(BlockedSlot.id)).where(BlockedSlot.start_at >= now)
        )
        has_any = (cnt or 0) > 0
    await message.answer(
        admin_block_root_text(has_any),
        reply_markup=build_admin_block_root_keyboard(),
    )


async def cb_admin_block_root(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    now = now_naive_local()
    with get_session() as session:
        cnt = session.scalar(
            select(func.count(BlockedSlot.id)).where(BlockedSlot.start_at >= now)
        )
        has_any = (cnt or 0) > 0
    await callback.message.edit_text(
        admin_block_root_text(has_any),
        reply_markup=build_admin_block_root_keyboard(),
    )
    await callback.answer()


async def cb_admin_block_day(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data or not callback.data.startswith("admblockday:"):
        await callback.answer()
        return
    _, iso = callback.data.split(":", 1)
    try:
        target_date = date.fromisoformat(iso)
    except ValueError:
        await callback.answer("Неверная дата.", show_alert=True)
        return

    with get_session() as session:
        blocked_stmt = (
            select(BlockedSlot.start_at)
            .where(func.date(BlockedSlot.start_at) == target_date)
            .order_by(BlockedSlot.start_at)
        )
        blocked_starts = [row[0] for row in session.execute(blocked_stmt).all()]

    if blocked_starts:
        lines = [f"Блокировки на <b>{target_date.strftime('%d.%m.%Y')}</b>:", ""]
        for dt in blocked_starts:
            lines.append(f"• {dt.strftime('%H:%M')} — забронировано тренером")
        lines.append("")
        lines.append("Выбери новый час для блокировки:")
        body = "\n".join(lines)
    else:
        body = (
            f"Блокировки на <b>{target_date.strftime('%d.%m.%Y')}</b>:\n\n"
            "Пока блокировок нет.\n\n"
            "Выбери час для блокировки:"
        )

    await callback.message.edit_text(
        body,
        reply_markup=build_admin_block_day_keyboard(target_date, blocked_starts),
    )
    await callback.answer()


async def cb_admin_block_all(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    now = now_naive_local()
    with get_session() as session:
        rows = session.scalars(
            select(BlockedSlot)
            .where(BlockedSlot.start_at >= now)
            .order_by(BlockedSlot.start_at)
        ).all()
    if not rows:
        body = "Все предстоящие блокировки:\n\nНет активных блокировок."
    else:
        lines = ["Все предстоящие блокировки:", ""]
        for b in rows:
            lines.append(f"• {b.start_at.strftime('%d.%m %H:%M')}")
        body = "\n".join(lines)
    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=ADM_BLOCK_ROOT)]
        ]
    )
    await callback.message.edit_text(body, reply_markup=back_kb)
    await callback.answer()


async def cb_admin_block_time(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data or not callback.data.startswith("admblocktime:"):
        await callback.answer()
        return
    _, dt_s = callback.data.split(":", 1)
    try:
        selected_dt = datetime.fromisoformat(dt_s)
    except ValueError:
        await callback.answer("Неверный формат времени.", show_alert=True)
        return

    if selected_dt <= now_naive_local():
        await callback.answer("Нельзя блокировать прошедшее время.", show_alert=True)
        return

    with get_session() as session:
        exists = session.scalar(
            select(BlockedSlot).where(BlockedSlot.start_at == selected_dt)
        )
        if exists:
            await callback.answer("Этот час уже заблокирован.", show_alert=True)
            return

        day_training_slots = [
            row[0]
            for row in session.execute(
                select(Training.start_at).where(
                    and_(
                        Training.status == "scheduled",
                        func.date(Training.start_at) == selected_dt.date(),
                    )
                )
            ).all()
        ]
        if not is_slot_available_for_service(selected_dt, day_training_slots, SERVICE_TRAINING):
            await callback.answer(
                "На это время уже есть запись клиента. Выбери другой час.",
                show_alert=True,
            )
            return

        session.add(BlockedSlot(start_at=selected_dt))
        session.commit()

        blocked_stmt = (
            select(BlockedSlot.start_at)
            .where(func.date(BlockedSlot.start_at) == selected_dt.date())
            .order_by(BlockedSlot.start_at)
        )
        blocked_starts = [row[0] for row in session.execute(blocked_stmt).all()]

    lines = [
        f"✅ Час <b>{selected_dt.strftime('%d.%m %H:%M')}</b> забронирован тренером.",
        "",
        "Этот слот скрыт из записи для тренировок и массажа.",
        "",
        f"Блокировки на <b>{selected_dt.strftime('%d.%m.%Y')}</b>:",
        "",
    ]
    for dt in blocked_starts:
        lines.append(f"• {dt.strftime('%H:%M')} — забронировано тренером")
    lines.append("")
    lines.append("Выбери еще час для блокировки:")
    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=build_admin_block_day_keyboard(selected_dt.date(), blocked_starts),
    )
    await callback.answer("Слот заблокирован.")


async def cb_admin_all_bookings_root(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    service_type = (
        SERVICE_MASSAGE
        if callback.data == ADM_MASS_ROOT
        else SERVICE_TRAINING
    )
    with get_session() as session:
        cnt = session.scalar(
            select(func.count(Training.id)).where(
                and_(
                    Training.status == "scheduled",
                    Training.service_type == service_type,
                )
            )
        )
        has_any = (cnt or 0) > 0

    await callback.message.edit_text(
        admin_all_bookings_root_text(has_any, service_type),
        reply_markup=build_admin_all_bookings_keyboard(service_type),
    )
    await callback.answer()


async def cb_admin_all_bookings_day(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    if not callback.data or not (
        callback.data.startswith("admtrbookday:") or callback.data.startswith("admmassday:")
    ):
        await callback.answer()
        return
    service_type = SERVICE_MASSAGE if callback.data.startswith("admmassday:") else SERVICE_TRAINING
    _, iso = callback.data.split(":", 1)
    try:
        target_date = date.fromisoformat(iso)
    except ValueError:
        await callback.answer("Неверная дата.", show_alert=True)
        return

    with get_session() as session:
        stmt = (
            select(Training)
            .options(selectinload(Training.user))
            .where(
                and_(
                    Training.status == "scheduled",
                    Training.service_type == service_type,
                    func.date(Training.start_at) == target_date,
                )
            )
            .order_by(Training.start_at)
        )
        rows = session.scalars(stmt).all()

    if not rows:
        body = (
            f"Записи на <b>{target_date.strftime('%d.%m.%Y')}</b>:\n\n"
            f"На этот день нет записей."
        )
    else:
        lines = [
            f"Записи на <b>{target_date.strftime('%d.%m.%Y')}</b>:",
            "",
        ]
        for t in rows:
            un = t.user.username or t.user.first_name or "—"
            lines.append(
                f"• {t.start_at.strftime('%H:%M')} — @{un} — {status_label(t.status)}"
            )
        body = "\n".join(lines)

    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=ADM_MASS_ROOT if service_type == SERVICE_MASSAGE else ADM_ALL_ROOT)]
        ]
    )
    await callback.message.edit_text(body, reply_markup=back_kb)
    await callback.answer()


async def cb_admin_all_bookings_all(callback: CallbackQuery):
    if not await reject_unless_admin(callback):
        return
    service_type = SERVICE_MASSAGE if callback.data == ADM_MASS_ALL else SERVICE_TRAINING
    now = now_naive_local()
    with get_session() as session:
        stmt = (
            select(Training)
            .options(selectinload(Training.user))
            .where(
                and_(
                    Training.status == "scheduled",
                    Training.service_type == service_type,
                    Training.start_at >= now,
                )
            )
            .order_by(Training.start_at)
        )
        rows = session.scalars(stmt).all()

    if not rows:
        body = (
            "Все предстоящие записи (Массаж):\n\nНет активных записей."
            if service_type == SERVICE_MASSAGE
            else "Все предстоящие записи (Тренировки):\n\nНет активных записей."
        )
    else:
        lines = [
            "Все предстоящие записи (Массаж):" if service_type == SERVICE_MASSAGE else "Все предстоящие записи (Тренировки):",
            "",
        ]
        for t in rows:
            un = t.user.username or t.user.first_name or "—"
            lines.append(
                f"• {t.start_at.strftime('%d.%m %H:%M')} (@{un}) — {status_label(t.status)}"
            )
        body = "\n".join(lines)

    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=ADM_MASS_ROOT if service_type == SERVICE_MASSAGE else ADM_ALL_ROOT)]
        ]
    )
    await callback.message.edit_text(body, reply_markup=back_kb)
    await callback.answer()


async def cb_training_passed_yes(callback: CallbackQuery):
    """Админ подтвердил: тренировка прошла."""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    if not callback.data:
        await callback.answer()
        return
    _, tid_s = callback.data.split(":", 1)
    tid = int(tid_s)
    with get_session() as session:
        t = session.get(Training, tid)
        if not t or t.status != "scheduled":
            await callback.answer("Запись уже обработана.", show_alert=True)
            return
        t.status = "completed"
        session.commit()
    base = callback.message.text or ""
    await callback.message.edit_text(
        base + "\n\n✅ Отмечено: тренировка прошла.",
        reply_markup=None,
    )
    await callback.answer()


async def cb_training_passed_no(callback: CallbackQuery):
    """Админ: тренировка не прошла — выбор отмены с возвратом или переноса."""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    if not callback.data:
        await callback.answer()
        return
    _, tid_s = callback.data.split(":", 1)
    tid = int(tid_s)
    builder = InlineKeyboardBuilder()
    builder.button(
        text="↩️ Отменить и вернуть в пакет",
        callback_data=f"tpr:{tid}:c",
    )
    builder.button(
        text="📅 Перенести",
        callback_data=f"tpr:{tid}:m",
    )
    builder.adjust(1)
    base = callback.message.text or ""
    await callback.message.edit_text(
        base + "\n\nТренировка не состоялась. Что сделать?",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


async def cb_training_post_resolve(callback: CallbackQuery):
    """tpr:ID:c — отмена с возвратом; tpr:ID:m — перенос."""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    if not callback.data:
        await callback.answer()
        return
    parts = callback.data.split(":")
    if len(parts) != 3 or parts[0] != "tpr":
        await callback.answer()
        return
    tid = int(parts[1])
    action = parts[2]
    today = today_local()

    if action == "c":
        with get_session() as session:
            t = session.get(Training, tid)
            if not t or t.status != "scheduled":
                await callback.answer("Уже обработано.", show_alert=True)
                return
            client = session.scalar(select(User).where(User.id == t.user_id))
            t.status = "cancelled"
            service_type = (t.service_type or SERVICE_TRAINING)
            if service_type == SERVICE_MASSAGE:
                client.massage_package_remaining += 1
            else:
                client.package_remaining += 1
            session.commit()
            client_tg = client.tg_id
            when = t.start_at.strftime("%d.%m %H:%M")
        base = callback.message.text or ""
        await callback.message.edit_text(
            base + "\n\n↩️ Запись отменена, занятие возвращено в пакет клиента.",
            reply_markup=None,
        )
        try:
            await callback.bot.send_message(
                chat_id=client_tg,
                text=(
                    f"Запись {when} отменена тренером (не состоялась). "
                    f"Сеанс возвращен в твой пакет."
                ),
            )
        except Exception as e:
            logging.exception(f"Notify client cancel: {e}")
        await callback.answer()
        return

    if action == "m":
        with get_session() as session:
            t = session.get(Training, tid)
            service_type = (t.service_type if t else SERVICE_TRAINING) or SERVICE_TRAINING
        user_states[callback.from_user.id] = {
            "flow": "admin_reschedule",
            "training_id": str(tid),
            "service_type": service_type,
        }
        base = callback.message.text or ""
        await callback.message.edit_text(
            base + "\n\n📅 Перенос: выбери дату в новом сообщении.",
            reply_markup=None,
        )
        await callback.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"📅 {calendar_title(today)}\n\n"
                f"Выбери новую дату для переноса тренировки #{tid}:"
            ),
            reply_markup=generate_calendar_keyboard(today),
        )
        await callback.answer()
        return

    await callback.answer()


async def reminders_worker(bot: Bot):
    while True:
        try:
            now = now_naive_local()
            window_start = now + timedelta(hours=2)
            window_end = window_start + timedelta(minutes=1)

            with get_session() as session:
                stmt = (
                    select(Training)
                    .where(
                        and_(
                            Training.status == "scheduled",
                            Training.start_at >= window_start,
                            Training.start_at < window_end,
                        )
                    )
                )
                trainings = session.scalars(stmt).all()

                for t in trainings:
                    if not t.reminder_client_sent:
                        try:
                            bot_text = (
                                f"Напоминание! Через 2 часа у тебя тренировка "
                                f"{t.start_at.strftime('%d.%m %H:%M')}."
                            )
                            await bot.send_message(chat_id=t.user.tg_id, text=bot_text)
                            t.reminder_client_sent = True
                        except Exception as e:
                            logging.exception(f"Failed to send client reminder: {e}")

                    if not t.reminder_admin_sent:
                        try:
                            admin_text = (
                                f"Напоминание! Через 2 часа тренировка с "
                                f"@{t.user.username or t.user.first_name} "
                                f"{t.start_at.strftime('%d.%m %H:%M')}."
                            )
                            await bot.send_message(chat_id=ADMIN_ID, text=admin_text)
                            t.reminder_admin_sent = True
                        except Exception as e:
                            logging.exception(f"Failed to send admin reminder: {e}")

                # Через 1 час после начала слота — спросить админа, прошла ли тренировка
                follow_cutoff = now - timedelta(hours=1)
                stmt_follow = (
                    select(Training)
                    .options(selectinload(Training.user))
                    .where(
                        and_(
                            Training.status == "scheduled",
                            Training.start_at <= follow_cutoff,
                            or_(
                                Training.post_session_prompt_sent == False,
                                Training.post_session_prompt_sent.is_(None),
                            ),
                        ),
                    )
                )
                for t in session.scalars(stmt_follow).all():
                    try:
                        u = t.user
                        uname = u.username or u.first_name or "клиент"
                        fb = InlineKeyboardBuilder()
                        fb.button(text="✅ Да, прошла", callback_data=f"tpy:{t.id}")
                        fb.button(text="❌ Нет", callback_data=f"tpn:{t.id}")
                        fb.adjust(2)
                        await bot.send_message(
                            chat_id=ADMIN_ID,
                            text=(
                                f"⏱ Тренировка {t.start_at.strftime('%d.%m %H:%M')} "
                                f"с @{uname} уже должна была начаться (прошёл час).\n\n"
                                f"Она прошла?"
                            ),
                            reply_markup=fb.as_markup(),
                        )
                        t.post_session_prompt_sent = True
                    except Exception as e:
                        logging.exception(f"Failed post-training follow-up: {e}")

                session.commit()
        except Exception as e:
            # Защита фонового worker от падения при временных сетевых сбоях.
            logging.exception(f"reminders_worker loop error: {e}")

        await asyncio.sleep(60)


async def main():
    # Aiogram's AiohttpSession не принимает параметр connector напрямую (передача ломает init).
    # Поэтому выставляем параметры TCPConnector через приватный _connector_init.
    session = AiohttpSession()
    try:
        session._connector_init["family"] = socket.AF_INET  # type: ignore[attr-defined]
    except Exception:
        pass
    bot = Bot(
        token=API_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML"),
        session=session,
    )
    dp = Dispatcher()

    if os.environ.get("BOOKING_BOT_WIPE_DB", "").strip().lower() in ("1", "true", "yes"):
        wipe_database()
        logging.info("BOOKING_BOT_WIPE_DB: таблицы users/trainings очищены.")
    sync_all_admin_flags()

    dp.message.register(cmd_start, CommandStart())
    dp.message.register(handle_contact, F.contact)

    async def router_message_handler(message: Message):
        try:
            user = await ensure_user(message)
            if user.is_admin and is_channel_admin(message.from_user.id):
                handled = await handle_admin_text(message)
                if handled:
                    return
            await handle_main_menu(message)
        except Exception as e:
            logging.exception(f"Unhandled error in message handler: {e}")
            # безопасное сообщение пользователю
            try:
                await message.answer(
                    "Что-то пошло не так, но бот уже работает дальше. "
                    "Попробуй ещё раз или воспользуйся кнопками меню."
                )
            except Exception:
                pass

    dp.message.register(router_message_handler, F.text)

    async def safe_callback_wrapper(callback: CallbackQuery, handler):
        try:
            await handler(callback)
        except Exception as e:
            logging.exception(f"Unhandled error in callback handler: {e}")
            try:
                await callback.answer("Произошла ошибка, попробуй ещё раз чуть позже.", show_alert=True)
            except Exception:
                pass

    # Aiogram передаёт в хендлеры доп. kwargs — лямбды с 2 аргументами ломали callback (бот «висел»).
    async def w_cal(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_calendar_navigation)

    async def w_date(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_select_date)

    async def w_time(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_select_time)

    async def w_cancel_flow(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_cancel_booking_flow)

    async def w_back_dates(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_back_to_dates)

    async def w_edit_my(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_edit_my)

    async def w_cancel_my(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_cancel_my)

    async def w_resched_my(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_reschedule_my)

    async def w_clients_page(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_clients_page)

    async def w_client_card(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_client_card)

    async def w_setpkg(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_set_package)

    async def w_setpkg_mass(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_set_massage_package)

    async def w_client_tr(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_client_trainings)

    async def w_admtr(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_training)

    async def w_admcancel(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_cancel_training)

    async def w_admresch(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_reschedule_training)

    async def w_adm_all_root(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_all_bookings_root)

    async def w_adm_all_all(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_all_bookings_all)

    async def w_adm_all_day(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_all_bookings_day)

    async def w_adm_block_root(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_block_root)

    async def w_adm_block_day(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_block_day)

    async def w_adm_block_all(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_block_all)

    async def w_adm_block_time(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_block_time)

    async def w_close(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_close_msg)

    async def w_tpy(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_training_passed_yes)

    async def w_tpn(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_training_passed_no)

    async def w_tpr(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_training_post_resolve)

    async def w_mybook_root(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_my_bookings_root)

    async def w_mybook_all(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_my_bookings_all)

    async def w_mybook_day(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_my_bookings_day)

    async def w_mymass_root(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_my_massage_root)

    async def w_mymass_all(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_my_massage_all)

    async def w_mymass_day(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_my_massage_day)

    dp.callback_query.register(w_cal, F.data.startswith("cal:"))
    dp.callback_query.register(w_date, F.data.startswith("date:"))
    dp.callback_query.register(w_time, F.data.startswith("time:"))
    dp.callback_query.register(w_cancel_flow, F.data == "cancel_booking_flow")
    dp.callback_query.register(w_back_dates, F.data == "back_to_dates")
    dp.callback_query.register(w_edit_my, F.data.startswith("edit_my:"))
    dp.callback_query.register(w_cancel_my, F.data.startswith("cancel_my:"))
    dp.callback_query.register(w_resched_my, F.data.startswith("reschedule_my:"))
    dp.callback_query.register(w_clients_page, F.data.startswith("clients_page:"))
    dp.callback_query.register(w_client_card, F.data.startswith("client:"))
    dp.callback_query.register(w_setpkg, F.data.startswith("setpkg:"))
    dp.callback_query.register(w_setpkg_mass, F.data.startswith("setpkgmass:"))
    dp.callback_query.register(w_client_tr, F.data.startswith("client_tr:"))
    dp.callback_query.register(w_admtr, F.data.startswith("admtr:"))
    dp.callback_query.register(w_admcancel, F.data.startswith("admcancel:"))
    dp.callback_query.register(w_admresch, F.data.startswith("admresch:"))
    dp.callback_query.register(w_adm_all_root, F.data == ADM_ALL_ROOT)
    dp.callback_query.register(w_adm_all_root, F.data == ADM_MASS_ROOT)
    dp.callback_query.register(w_adm_all_all, F.data == ADM_ALL_ALL)
    dp.callback_query.register(w_adm_all_all, F.data == ADM_MASS_ALL)
    dp.callback_query.register(w_adm_all_day, F.data.startswith("admtrbookday:"))
    dp.callback_query.register(w_adm_all_day, F.data.startswith("admmassday:"))
    dp.callback_query.register(w_adm_block_root, F.data == ADM_BLOCK_ROOT)
    dp.callback_query.register(w_adm_block_all, F.data == ADM_BLOCK_ALL)
    dp.callback_query.register(w_adm_block_day, F.data.startswith("admblockday:"))
    dp.callback_query.register(w_adm_block_time, F.data.startswith("admblocktime:"))
    dp.callback_query.register(w_close, F.data == "close_msg")
    dp.callback_query.register(w_tpy, F.data.startswith("tpy:"))
    dp.callback_query.register(w_tpn, F.data.startswith("tpn:"))
    dp.callback_query.register(w_tpr, F.data.startswith("tpr:"))
    dp.callback_query.register(w_mybook_root, F.data == MY_BOOK_ROOT)
    dp.callback_query.register(w_mybook_all, F.data == MY_BOOK_ALL)
    dp.callback_query.register(w_mybook_day, F.data.startswith("mybookday:"))
    dp.callback_query.register(w_mymass_root, F.data == MY_MASS_ROOT)
    dp.callback_query.register(w_mymass_all, F.data == MY_MASS_ALL)
    dp.callback_query.register(w_mymass_day, F.data.startswith("mymassday:"))

    asyncio.create_task(reminders_worker(bot))

    logging.info("Bot started.")
    while True:
        try:
            await dp.start_polling(bot)
            break
        except Exception as e:
            logging.exception(f"Polling crashed, retrying in {POLLING_RETRY_SECONDS}s: {e}")
            await asyncio.sleep(POLLING_RETRY_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")

