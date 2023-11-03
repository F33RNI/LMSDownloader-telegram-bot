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
import argparse
import logging
import multiprocessing
import os
import sys

import BotHandler
import LMSDownloaderHandler
import LoggingHandler
from JSONReaderWriter import load_json

# LMSDownloader-telegram-bot version
__version__ = "1.0.0"

# Logging level
LOGGING_LEVEL = logging.INFO

# Default config file
CONFIG_FILE = "config.json"


def parse_args():
    """
    Parses cli arguments
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="config.json file location",
                        default=os.getenv("LMSDOWNLOADER_TELEGRAM_BOT_CONFIG_FILE", CONFIG_FILE))
    parser.add_argument("--version", action="version", version=__version__)
    return parser.parse_args()


def main():
    # Multiprocessing fix for Windows
    if sys.platform.startswith("win"):
        multiprocessing.freeze_support()

    # Initialize logging and start listener as process
    logging_handler = LoggingHandler.LoggingHandler()
    logging_handler_process = multiprocessing.Process(target=logging_handler.configure_and_start_listener)
    logging_handler_process.start()
    LoggingHandler.worker_configurer(logging_handler.queue)
    logging.info("LoggingHandler PID: " + str(logging_handler_process.pid))

    # Log software version and GitHub link
    logging.info("LMSDownloader-telegram-bot version: " + str(__version__))
    logging.info("https://github.com/F33RNI/LMSDownloader-telegram-bot")

    # Parse arguments
    args = parse_args()

    # Load config with multiprocessing support
    config = multiprocessing.Manager().dict(load_json(args.config))

    # Load messages from json file with multiprocessing support
    messages = multiprocessing.Manager().dict(load_json(config["messages_file"]))

    # Initialize LMSDownloader handler class
    lms_downloader_handler = LMSDownloaderHandler.LMSDownloaderHandler(config, messages, logging_handler)
    lms_downloader_handler.processes_watchdog_start()

    # Initialize telegram bot class
    bot_handler = BotHandler.BotHandler(config, messages, logging_handler.queue, lms_downloader_handler)

    # Finally, start telegram bot in main thread
    bot_handler.start_bot()

    # If we're here, exit requested
    lms_downloader_handler.processes_watchdog_stop()
    logging.info("LMSDownloader-telegram-bot exited successfully")

    # Finally, stop logging loop
    logging_handler.queue.put(None)


if __name__ == "__main__":
    main()
