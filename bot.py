import asyncio
import logging
from datetime import datetime, timedelta, time, date
from typing import Optional, List, Dict

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
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

API_TOKEN = "7348147274:AAEqXWiK10yRvk36Pe3xtWuNl_ac_FqSMqc"
ADMIN_ID = 1652603985

DATABASE_URL = "sqlite:///booking_bot.db"

# Минимум за сколько до начала слота можно записаться / перенести (новое время)
BOOKING_MIN_ADVANCE = timedelta(hours=1)

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
    created_at = Column(DateTime, default=datetime.utcnow)

    trainings = relationship("Training", back_populates="user")


class Training(Base):
    __tablename__ = "trainings"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
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


Base.metadata.create_all(bind=engine)


def ensure_db_columns():
    """Добавить колонки в существующую SQLite-БД (create_all не меняет старые таблицы)."""
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(trainings)")).fetchall()
        col_names = {r[1] for r in rows}
        if "post_session_prompt_sent" not in col_names:
            conn.execute(
                text(
                    "ALTER TABLE trainings ADD COLUMN post_session_prompt_sent BOOLEAN DEFAULT 0"
                )
            )


ensure_db_columns()


def get_session() -> Session:
    return SessionLocal()


def main_menu_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="Тренировки")],
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
        [KeyboardButton(text="⬅️ Назад в меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_menu_kb() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="👥 Клиенты")],
        [KeyboardButton(text="📆 Все записи")],
        [KeyboardButton(text="⬅️ Назад в меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


MONTH_NAMES = (
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
)


def calendar_title(current_date: date) -> str:
    """Заголовок для календаря: месяц и год."""
    return f"{MONTH_NAMES[current_date.month - 1]} {current_date.year}"


def generate_calendar_keyboard(current_date: date) -> InlineKeyboardMarkup:
    today = date.today()
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


def earliest_bookable_moment() -> datetime:
    """Слот доступен только если до его начала не меньше BOOKING_MIN_ADVANCE."""
    return datetime.now() + BOOKING_MIN_ADVANCE


def generate_time_keyboard(selected_date: date, existing_slots: List[datetime]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    start_time = time(9, 0)
    end_time = time(19, 0)

    occupied_times = {dt.time() for dt in existing_slots}

    current_datetime = datetime.now()
    min_slot_start = earliest_bookable_moment()

    dt = datetime.combine(selected_date, start_time)
    while dt.time() <= end_time:
        if selected_date == current_datetime.date() and dt <= current_datetime:
            dt += timedelta(minutes=15)
            continue

        # Нельзя записаться «впритык»: минимум за час до начала
        if dt < min_slot_start:
            dt += timedelta(minutes=15)
            continue

        if dt.time() not in occupied_times:
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
                is_admin=message.from_user.id == ADMIN_ID,
            )
            session.add(user)
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

    await message.answer(
        "Регистрация завершена! Теперь ты можешь записываться на тренировки.",
        reply_markup=main_menu_kb(is_admin=user.is_admin if user else False),
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
    elif text == "Мой пакет":
        await send_my_package(message, user)
    elif text == "🛠 Админ панель" and user.is_admin:
        await message.answer("Админ панель:", reply_markup=admin_menu_kb())
    elif text == "⬅️ Назад в меню":
        await message.answer(
            "Главное меню:",
            reply_markup=main_menu_kb(is_admin=user.is_admin),
        )
    elif text == "📝 Записаться на тренировку":
        await start_booking_flow(message, user)
    elif text == "❌ Отменить/перенести запись":
        await start_cancel_reschedule_flow(message, user)
    elif text == "📋 Мои записи":
        await send_my_bookings(message, user)
    elif text == "👥 Клиенты" and user.is_admin:
        await admin_show_clients(message, page=0)
    elif text == "📆 Все записи" and user.is_admin:
        await admin_show_all_trainings(message, page=0)
    else:
        await message.answer(
            "Не понял команду. Используй кнопки меню ниже.",
            reply_markup=main_menu_kb(is_admin=user.is_admin),
        )


async def send_my_package(message: Message, user: User):
    text = (
        f"📦 Твой пакет тренировок\n\n"
        f"Всего: {user.package_total}\n"
        f"Осталось: {user.package_remaining}"
    )
    if user.package_remaining > 0:
        text += "\n\nЗаписаться можно в разделе «Тренировки»."
    else:
        text += "\n\nЧтобы записаться, попроси тренера пополнить пакет."
    await message.answer(
        text,
        reply_markup=main_menu_kb(is_admin=user.is_admin),
    )


async def start_booking_flow(message: Message, user: User):
    if user.package_remaining <= 0:
        await message.answer(
            "У тебя нет доступных тренировок. Обратись к тренеру, чтобы пополнить пакет.",
            reply_markup=main_menu_kb(is_admin=user.is_admin),
        )
        return

    user_states[message.from_user.id] = {"flow": "booking"}

    today = date.today()
    await message.answer(
        f"📅 {calendar_title(today)}\n\n"
        f"Выбери дату для тренировки.\n"
        f"⏱ Запись только не позднее чем за <b>1 час</b> до начала слота.",
        reply_markup=generate_calendar_keyboard(today),
    )


async def start_cancel_reschedule_flow(message: Message, user: User):
    with get_session() as session:
        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == user.id,
                    Training.status == "scheduled",
                    Training.start_at >= datetime.now(),
                )
            )
            .order_by(Training.start_at)
        )
        trainings = session.scalars(stmt).all()

    if not trainings:
        await message.answer(
            "У тебя нет активных записей для отмены или переноса.",
            reply_markup=trainings_menu_kb(),
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
        "Выбери тренировку для отмены или переноса:",
        reply_markup=builder.as_markup(),
    )


async def send_my_bookings(message: Message, user: User):
    now = datetime.now()
    with get_session() as session:
        stmt = (
            select(Training)
            .where(
                and_(
                    Training.user_id == user.id,
                    Training.status == "scheduled",
                    Training.start_at >= now,
                )
            )
            .order_by(Training.start_at)
        )
        trainings = session.scalars(stmt).all()

    if not trainings:
        await message.answer(
            "У тебя нет предстоящих записей. Записаться можно через «Записаться на тренировку».",
            reply_markup=trainings_menu_kb(),
        )
        return

    lines = []
    for t in trainings:
        lines.append(
            f"✅ {t.start_at.strftime('%d.%m %H:%M')} — {status_label(t.status)}"
        )

    await message.answer(
        "Твои предстоящие записи:\n\n" + "\n".join(lines),
        reply_markup=trainings_menu_kb(),
    )


async def cb_calendar_navigation(callback: CallbackQuery):
    if not callback.data:
        return
    _, date_str = callback.data.split(":", 1)
    target_date = date.fromisoformat(date_str)
    await callback.message.edit_text(
        f"📅 {calendar_title(target_date)}\n\nВыбери дату для тренировки:",
        reply_markup=generate_calendar_keyboard(target_date),
    )
    await callback.answer()


async def cb_cancel_booking_flow(callback: CallbackQuery):
    user_states.pop(callback.from_user.id, None)
    await callback.message.edit_text("Запись на тренировку отменена.")
    try:
        await callback.bot.send_message(
            chat_id=callback.from_user.id,
            text="Выбери действие в меню «Тренировки»:",
            reply_markup=trainings_menu_kb(),
        )
    except Exception:
        pass
    await callback.answer()


async def cb_back_to_dates(callback: CallbackQuery):
    today = date.today()
    await callback.message.edit_text(
        f"📅 {calendar_title(today)}\n\nВыбери дату для тренировки:",
        reply_markup=generate_calendar_keyboard(today),
    )
    await callback.answer()


async def cb_select_date(callback: CallbackQuery):
    if not callback.data:
        return
    _, date_str = callback.data.split(":", 1)
    selected_date = date.fromisoformat(date_str)

    with get_session() as session:
        stmt = (
            select(Training.start_at)
            .where(
                and_(
                    Training.status == "scheduled",
                    func.date(Training.start_at) == selected_date,
                )
            )
            .order_by(Training.start_at)
        )
        existing_slots = [row[0] for row in session.execute(stmt).all()]

    await callback.message.edit_text(
        f"Дата {selected_date.strftime('%d.%m')}. Выбери время:",
        reply_markup=generate_time_keyboard(selected_date, existing_slots),
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

    min_dt = earliest_bookable_moment()
    if flow in ("booking", "client_reschedule", "admin_reschedule"):
        if selected_dt < min_dt:
            await callback.answer(
                "Запись и перенос возможны только не позднее чем за 1 час до начала слота. "
                "Выбери другое время.",
                show_alert=True,
            )
            return

    with get_session() as session:
        user = session.scalar(select(User).where(User.tg_id == user_id))
        if not user:
            await callback.answer("Пользователь не найден.", show_alert=True)
            return

        if flow == "booking":
            stmt = select(Training).where(
                and_(
                    Training.status == "scheduled",
                    Training.start_at == selected_dt,
                )
            )
            existing = session.scalar(stmt)
            if existing:
                await callback.answer(
                    "Это время уже занято. Выбери другое.", show_alert=True
                )
                return

            if user.package_remaining <= 0:
                await callback.answer(
                    "У тебя нет доступных тренировок. Обратись к тренеру.", show_alert=True
                )
                return

            t = Training(user_id=user.id, start_at=selected_dt, status="scheduled")
            user.package_remaining -= 1
            session.add(t)
            session.commit()
            session.refresh(t)

            await callback.message.edit_text(
                f"✅ Ты записан на тренировку {selected_dt.strftime('%d.%m %H:%M')}.\n"
                f"Осталось тренировок в пакете: {user.package_remaining}",
            )
            try:
                await callback.bot.send_message(
                    chat_id=user_id,
                    text="Можешь записаться ещё или посмотреть «Мои записи».",
                    reply_markup=trainings_menu_kb(),
                )
            except Exception:
                pass

            await callback.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"Новая запись на тренировку:\n"
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

            stmt_exist = select(Training).where(
                and_(
                    Training.status == "scheduled",
                    Training.start_at == selected_dt,
                    Training.id != int(training_id),
                )
            )
            existing = session.scalar(stmt_exist)
            if existing:
                await callback.answer(
                    "Это время уже занято. Выбери другое.", show_alert=True
                )
                return

            t = session.get(Training, int(training_id))
            if not t or t.status != "scheduled":
                await callback.answer("Тренировка не найдена или уже изменена.", show_alert=True)
                return

            old_time = t.start_at
            t.start_at = selected_dt
            t.post_session_prompt_sent = False
            t.reminder_client_sent = False
            t.reminder_admin_sent = False
            session.commit()

            await callback.message.edit_text(
                f"✅ Тренировка перенесена с {old_time.strftime('%d.%m %H:%M')} "
                f"на {t.start_at.strftime('%d.%m %H:%M')}."
            )
            try:
                await callback.bot.send_message(
                    chat_id=user.tg_id,
                    text=(
                        f"Ты перенёс свою тренировку.\n"
                        f"Новое время: {t.start_at.strftime('%d.%m %H:%M')}"
                    ),
                    reply_markup=trainings_menu_kb(),
                )
            except Exception:
                pass
            await callback.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"Клиент @{user.username or user.first_name} перенёс тренировку.\n"
                    f"Новое время: {t.start_at.strftime('%d.%m %H:%M')}"
                ),
            )
            user_states.pop(user_id, None)
            await callback.answer()
        elif flow == "admin_reschedule":
            training_id = state.get("training_id")
            if not training_id:
                await callback.answer("Ошибка состояния переноса.", show_alert=True)
                return

            stmt_exist = select(Training).where(
                and_(
                    Training.status == "scheduled",
                    Training.start_at == selected_dt,
                    Training.id != int(training_id),
                )
            )
            existing = session.scalar(stmt_exist)
            if existing:
                await callback.answer(
                    "Это время уже занято. Выбери другое.", show_alert=True
                )
                return

            t = session.get(Training, int(training_id))
            if not t or t.status != "scheduled":
                await callback.answer("Тренировка не найдена или уже изменена.", show_alert=True)
                return

            old_time = t.start_at
            t.start_at = selected_dt
            t.post_session_prompt_sent = False
            t.reminder_client_sent = False
            t.reminder_admin_sent = False
            session.commit()

            await callback.message.edit_text(
                f"Тренировка перенесена с {old_time.strftime('%d.%m %H:%M')} "
                f"на {t.start_at.strftime('%d.%m %H:%M')}."
            )

            await callback.bot.send_message(
                chat_id=t.user.tg_id,
                text=(
                    f"Твоя тренировка была перенесена тренером.\n"
                    f"Новое время: {t.start_at.strftime('%d.%m %H:%M')}"
                ),
            )

            await callback.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"Ты перенёс тренировку клиента @{t.user.username or t.user.first_name}\n"
                    f"Новое время: {t.start_at.strftime('%d.%m %H:%M')}"
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

    now = datetime.now()

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
            user.package_remaining += 1

        session.commit()

        await callback.message.edit_text(
            f"❌ Тренировка на {t.start_at.strftime('%d.%m %H:%M')} отменена.\n"
            + ("Тренировка вернулась в твой пакет." if refundable else "Меньше чем за 4 часа — тренировка сгорает."),
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

        delta = t.start_at - datetime.now()
        if delta < timedelta(hours=4):
            await callback.answer(
                "Перенести можно не позднее чем за 4 часа до тренировки.", show_alert=True
            )
            return

    user_states[user_id] = {"flow": "client_reschedule", "training_id": str(training_id)}

    today = date.today()
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
        label += f" | осталось: {c.package_remaining}"
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
    if not callback.data:
        return
    _, page_str = callback.data.split(":", 1)
    page = int(page_str)
    await callback.message.delete()
    await admin_show_clients(callback.message, page=page)
    await callback.answer()


async def cb_client_card(callback: CallbackQuery):
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

    client_name = f"{client.first_name or ''} {client.last_name or ''}".strip()
    if not client_name:
        client_name = f"@{client.username}" if client.username else f"ID {client.tg_id}"
    caption_lines = [
        f"👤 Клиент: {client_name}",
        f"📱 Телефон: {client.phone or 'не указан'}",
        f"📦 Пакет: всего {client.package_total}, осталось {client.package_remaining}",
        "",
        "Предстоящие и ожидающие итога:",
    ]
    if trainings:
        now = datetime.now()
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


async def handle_admin_text(message: Message):
    state = admin_states.get(message.from_user.id)
    if not state:
        return False

    if state.get("action") == "set_package":
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

            diff = value - client.package_total
            client.package_total = value
            client.package_remaining = max(client.package_remaining + diff, 0)
            session.commit()

        await message.answer(
            f"Для клиента обновлён пакет: всего {value}, осталось {client.package_remaining}.",
            reply_markup=admin_menu_kb(),
        )
        admin_states.pop(message.from_user.id, None)
        return True

    return False


async def cb_client_trainings(callback: CallbackQuery):
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
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)

    with get_session() as session:
        t = session.get(Training, training_id)
        if not t:
            await callback.answer("Тренировка не найдена.", show_alert=True)
            return

        client = t.user

    builder = InlineKeyboardBuilder()
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
        InlineKeyboardButton(text="Назад", callback_data=f"client_tr:{client.id}")
    )

    await callback.message.edit_text(
        f"Тренировка ID {t.id}\n"
        f"Клиент: @{client.username or client.first_name}\n"
        f"Время: {t.start_at.strftime('%d.%m %H:%M')}\n"
        f"Статус: {status_label(t.status)}",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


async def cb_admin_cancel_training(callback: CallbackQuery):
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)

    now = datetime.now()

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
            client.package_remaining += 1

        session.commit()

    await callback.message.edit_text(
        f"Тренировка клиента @{client.username or client.first_name} "
        f"на {t.start_at.strftime('%d.%m %H:%M')} отменена тренером.\n"
        + ("Возврат в пакет." if refundable else "Меньше чем за 4 часа, тренировка сгорает."),
    )

    await callback.bot.send_message(
        chat_id=client.tg_id,
        text=(
            f"Твоя тренировка {t.start_at.strftime('%d.%m %H:%M')} была отменена тренером.\n"
            f"{'Тренировка вернулась в твой пакет.' if refundable else 'Меньше чем за 4 часа, тренировка сгорела.'}"
        ),
    )

    await callback.answer()


async def cb_admin_reschedule_training(callback: CallbackQuery):
    if not callback.data:
        return
    _, training_id_str = callback.data.split(":", 1)
    training_id = int(training_id_str)
    admin_id = callback.from_user.id

    user_states[admin_id] = {"flow": "admin_reschedule", "training_id": str(training_id)}

    today = date.today()
    await callback.message.edit_text(
        f"📅 {calendar_title(today)}\n\nВыбери новую дату для тренировки клиента:",
        reply_markup=generate_calendar_keyboard(today),
    )
    await callback.answer()


async def cb_close_msg(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()


async def admin_show_all_trainings(message: Message, page: int = 0):
    page_size = 10
    offset = page * page_size

    with get_session() as session:
        stmt_count = select(func.count(Training.id))
        total = session.scalar(stmt_count)

        stmt = (
            select(Training)
            .order_by(Training.start_at.desc())
            .offset(offset)
            .limit(page_size)
        )
        trainings = session.scalars(stmt).all()

        if not trainings:
            await message.answer("Записей пока нет.")
            return

        text_lines = [f"Все записи. Страница {page + 1}:"]
        for t in trainings:
            text_lines.append(
                f"{t.id}: {t.start_at.strftime('%d.%m %H:%M')} — {status_label(t.status)} — "
                f"@{t.user.username or t.user.first_name}"
            )

    builder = InlineKeyboardBuilder()
    if offset > 0:
        builder.button(
            text="⬅️ Назад",
            callback_data=f"alltr_page:{page - 1}",
        )
    if offset + page_size < total:
        builder.button(
            text="Вперёд ➡️",
            callback_data=f"alltr_page:{page + 1}",
        )
    builder.adjust(2)

    await message.answer(
        "\n".join(text_lines),
        reply_markup=builder.as_markup(),
    )


async def cb_all_trainings_page(callback: CallbackQuery):
    if not callback.data:
        return
    _, page_str = callback.data.split(":", 1)
    page = int(page_str)
    await callback.message.delete()
    await admin_show_all_trainings(callback.message, page=page)
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
    today = date.today()

    if action == "c":
        with get_session() as session:
            t = session.get(Training, tid)
            if not t or t.status != "scheduled":
                await callback.answer("Уже обработано.", show_alert=True)
                return
            client = session.scalar(select(User).where(User.id == t.user_id))
            t.status = "cancelled"
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
                    f"Тренировка {when} отменена тренером (не состоялась). "
                    f"Занятие возвращено в твой пакет."
                ),
            )
        except Exception as e:
            logging.exception(f"Notify client cancel: {e}")
        await callback.answer()
        return

    if action == "m":
        user_states[callback.from_user.id] = {
            "flow": "admin_reschedule",
            "training_id": str(tid),
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
        now = datetime.now()
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
                    )
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

        await asyncio.sleep(60)


async def main():
    bot = Bot(
        token=API_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML"),
    )
    dp = Dispatcher()

    dp.message.register(cmd_start, CommandStart())
    dp.message.register(handle_contact, F.contact)

    async def router_message_handler(message: Message):
        try:
            user = await ensure_user(message)
            if user.is_admin:
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

    async def w_client_tr(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_client_trainings)

    async def w_admtr(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_training)

    async def w_admcancel(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_cancel_training)

    async def w_admresch(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_admin_reschedule_training)

    async def w_alltr_page(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_all_trainings_page)

    async def w_close(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_close_msg)

    async def w_tpy(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_training_passed_yes)

    async def w_tpn(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_training_passed_no)

    async def w_tpr(cq: CallbackQuery, **_kw):
        await safe_callback_wrapper(cq, cb_training_post_resolve)

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
    dp.callback_query.register(w_client_tr, F.data.startswith("client_tr:"))
    dp.callback_query.register(w_admtr, F.data.startswith("admtr:"))
    dp.callback_query.register(w_admcancel, F.data.startswith("admcancel:"))
    dp.callback_query.register(w_admresch, F.data.startswith("admresch:"))
    dp.callback_query.register(w_alltr_page, F.data.startswith("alltr_page:"))
    dp.callback_query.register(w_close, F.data == "close_msg")
    dp.callback_query.register(w_tpy, F.data.startswith("tpy:"))
    dp.callback_query.register(w_tpn, F.data.startswith("tpn:"))
    dp.callback_query.register(w_tpr, F.data.startswith("tpr:"))

    asyncio.create_task(reminders_worker(bot))

    logging.info("Bot started.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")

