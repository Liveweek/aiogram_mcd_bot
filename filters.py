from aiogram.dispatcher.filters import Filter
from aiogram.types import Message

from config import ADMIN_IDS


class IsAdmin(Filter):
    key = "is_admin"

    async def check(self, message: Message) -> bool:
        return message.from_user.id in ADMIN_IDS

