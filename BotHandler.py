"""
 Copyright (C) 2023 Fern Lane, LMSDownloader-telegram-bot

 Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

       https://www.gnu.org/licenses/agpl-3.0.en.html

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR
 OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 OTHER DEALINGS IN THE SOFTWARE.
"""
import asyncio
import logging
import multiprocessing
import re
import signal
import time
from typing import Any

import telegram
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, filters, MessageHandler, ContextTypes, CallbackQueryHandler

import LMSDownloaderHandler
from main import __version__

# Bot commands
BOT_COMMAND_START = "start"

# After how long (in seconds) restart the bot if connection failed
BOT_RESTART_ON_NETWORK_ERROR = 10.

# List of markdown chars to escape with \\
MARKDOWN_ESCAPE = ["_", "*", "[", "]", "(", ")", "~", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"]

# Callbacks definitions
CALLBACK_START = 0
CALLBACK_MESSAGE = 1
CALLBACK_QUERY = 2


def async_helper(awaitable_) -> None:
    """
    Runs async function inside sync
    :param awaitable_:
    :return:
    """
    # Try to get current event loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    # Check it
    if loop and loop.is_running():
        loop.create_task(awaitable_)

    # We need new event loop
    else:
        asyncio.run(awaitable_)


def build_menu(buttons, n_cols=1, header_buttons=None, footer_buttons=None) -> list[list[Any]]:
    """
    Returns a list of inline buttons used to generate inlinekeyboard responses
    :param buttons: list of InlineKeyboardButton
    :param n_cols: Number of columns (number of list of buttons)
    :param header_buttons: First button value
    :param footer_buttons: Last button value
    :return: list of inline buttons
    """
    buttons = [button for button in buttons if button is not None]
    menu = [buttons[i: i + n_cols] for i in range(0, len(buttons), n_cols)]
    if header_buttons:
        menu.insert(0, header_buttons)
    if footer_buttons:
        menu.append(footer_buttons)
    return menu


async def send_safe(chat_id: int, text: str, context: ContextTypes.DEFAULT_TYPE or None,
                    reply_to_message_id=None,
                    reply_markup=None,
                    parse_markdown=False,
                    edit_message_id=None,
                    bot_api_token="") -> int or None:
    """
    Sends message without raising any error
    :param chat_id: User (chat) ID
    :param text: Message text
    :param context: ContextTypes.DEFAULT_TYPE for context.bot or None for token
    :param reply_to_message_id: Message ID to reply to
    :param reply_markup: Buttons for message
    :param parse_markdown: True to parse message as markdown
    :param edit_message_id: None for new message or ID for edit it
    :param bot_api_token: Use token instead of context.bot
    :return: message ID or None (in case of error)
    """
    try:
        # Escape all chars
        if parse_markdown:
            for i in range(len(MARKDOWN_ESCAPE)):
                escape_char = MARKDOWN_ESCAPE[i]
                text = text.replace(escape_char, "\\" + escape_char)

        if context:
            if not edit_message_id:
                return (await context.bot.send_message(chat_id=chat_id,
                                                       text=text.replace("\\n", "\n").replace("\\t", "\t"),
                                                       reply_to_message_id=reply_to_message_id,
                                                       reply_markup=reply_markup,
                                                       disable_web_page_preview=True,
                                                       parse_mode="MarkdownV2" if
                                                       parse_markdown else None)).message_id
            else:
                return (await context.bot.edit_message_text(chat_id=chat_id,
                                                            message_id=edit_message_id,
                                                            text=text.replace("\\n", "\n").replace("\\t", "\t"),
                                                            reply_markup=reply_markup,
                                                            disable_web_page_preview=True,
                                                            parse_mode="MarkdownV2" if
                                                            parse_markdown else None)).message_id
        else:
            if not edit_message_id:
                return (await telegram.Bot(bot_api_token).send_message(chat_id=chat_id,
                                                                       text=text.replace("\\n", "\n").replace("\\t",
                                                                                                              "\t"),
                                                                       reply_to_message_id=reply_to_message_id,
                                                                       reply_markup=reply_markup,
                                                                       disable_web_page_preview=True,
                                                                       parse_mode="MarkdownV2" if
                                                                       parse_markdown else None)).message_id
            else:
                return (await telegram.Bot(bot_api_token).edit_message_text(chat_id=chat_id,
                                                                            message_id=edit_message_id,
                                                                            text=text.replace("\\n", "\n").replace(
                                                                                "\\t", "\t"),
                                                                            reply_markup=reply_markup,
                                                                            disable_web_page_preview=True,
                                                                            parse_mode="MarkdownV2" if
                                                                            parse_markdown else None)).message_id
    except Exception as e:
        logging.error("Error sending {0} to {1}!".format(text.replace("\\n", "\n").replace("\\t", "\t"), chat_id),
                      exc_info=e)
    return None


class BotHandler:
    def __init__(self, config: dict or multiprocessing.Manager().dict,
                 messages: dict or multiprocessing.Manager().dict,
                 logging_queue: multiprocessing.Queue,
                 lms_downloader_handler: LMSDownloaderHandler.LMSDownloaderHandler) -> None:
        self._config = config
        self._messages = messages
        self._logging_queue = logging_queue
        self._lms_downloader_handler = lms_downloader_handler

        self.application = None
        self._application_stopped = False

    def _stop_handler(self, *args):
        """
        Signals callback (really stops telegram bot)
        :param args:
        :return:
        """
        if self.application and not self._application_stopped:
            logging.warning("Stopping telegram bot. Please wait")
            self._application_stopped = True
            async_helper(self.application.stop())
            asyncio.get_event_loop().stop()

    def start_bot(self) -> None:
        """
        Starts telegram bot (blocking)
        Send SIGINT or SIGTERM to stop it
        :return:
        """
        try:
            signal.signal(signal.SIGINT, self._stop_handler)
        except Exception as e:
            logging.warning("Can't connect SIGINT signal: {}".format(e))
        try:
            signal.signal(signal.SIGTERM, self._stop_handler)
        except Exception as e:
            logging.warning("Can't connect SIGTERM signal: {}".format(e))

        # Start telegram bot polling
        logging.info("Starting telegram bot")
        while True:
            try:
                # Build bot
                builder = ApplicationBuilder().token(self._config["bot_api_token"])
                self.application = builder.build()

                # User commands
                self.application.add_handler(CommandHandler(BOT_COMMAND_START, self.bot_command_start))

                # Handle requests as messages
                self.application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), self.bot_message))

                # Unknown command -> send start
                self.application.add_handler(MessageHandler(filters.COMMAND, self.bot_command_start))

                # Add buttons handler
                self.application.add_handler(CallbackQueryHandler(self.query_callback))

                # Start bot and ignore any signals inside event loop
                self._application_stopped = False
                self.application.run_polling(close_loop=False, stop_signals=[])

                # Exit or restart bot
                if self._application_stopped:
                    break
                logging.warning("Restarting telegram bot after 1s")
                time.sleep(1)

            # Exit requested (in case of manual CTRL+C spamming)
            except (KeyboardInterrupt, SystemExit):
                logging.warning("Telegram bot interrupted!")
                break

            # Couldn't connect
            except telegram.error.NetworkError:
                logging.warning("NetworkError. Restarting bot after {:.1f}s".format(BOT_RESTART_ON_NETWORK_ERROR))
                time.sleep(BOT_RESTART_ON_NETWORK_ERROR)

            # Bot error?
            except Exception as e:
                logging.error("Telegram bot interrupted or error occurred!", exc_info=e)
                break

        # If we're here, exit requested
        logging.warning("Telegram bot stopped")

    async def bot_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Message from user
        :param update:
        :param context:
        :return:
        """
        user_id = update.effective_chat.id
        logging.info("Request from {0}".format(user_id))

        # Read message
        try:
            # Split message by new lines
            request_message = update.message.text.strip().replace("\r", "").split("\n")

            # Parse and check each part
            request_login = request_message[0].strip()
            assert len(request_login) > 0
            request_password = request_message[1].strip()
            assert len(request_password) > 0
            request_link = request_message[2].strip()
            assert len(request_link) > 0

            # Check link
            if re.search(self._config["link_check_regex"], request_link) is None:
                logging.warning("Wrong link format!")
                await send_safe(user_id, self._messages["wrong_link"].format(self._config["link_check_regex"]),
                                context,
                                parse_markdown=True)
                return

        except Exception as e:
            logging.warning("Error parsing message: {}".format(str(e)))
            await send_safe(user_id, self._messages["wrong_message"].format(str(e)), context, parse_markdown=True)
            return

        self._lms_downloader_handler.start_new_process(request_login, request_password, request_link, user_id)

    async def bot_command_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        /start command
        :param update:
        :param context:
        :return:
        """
        user_id = update.effective_chat.id
        logging.info("/start command from {0}".format(user_id))
        await send_safe(user_id, self._messages["start_message"].format(__version__), context)

    async def query_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        reply_markup buttons callback
        :param update:
        :param context:
        :return:
        """
        try:
            telegram_chat_id = update.effective_chat.id
            data_ = update.callback_query.data
            if telegram_chat_id and data_:
                # Parse data from button
                data_splitted = data_.split("_")
                action = data_splitted[0]
                pid = int(data_splitted[1])

                # Abort process
                if action == "abort":
                    self._lms_downloader_handler.interrupt_process(pid)

        # Error parsing data?
        except Exception as e:
            logging.error("Query callback error!", exc_info=e)
