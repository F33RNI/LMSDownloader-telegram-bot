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
import os
import queue
import signal
import tempfile
import threading
import time

import telegram
from LMSDownloader import LMSDownloader
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import BotHandler
import LoggingHandler

# How long to wait for all processes to finish if exit requested
EXIT_WAIT_TIME = 5.0

# How long to wait for child process to terminate
CHILD_PROCESS_MAX_TERMINATE_TIME = 1.0

# Timeouts for sending file
FILE_SEND_TIMEOUTS = 60

# How long to wait before trying to send file again
RESEND_FILE_AFTER_TIME = 3.0

# Maximum retries to send file
MAX_FILES_RETRIES = 3


def _lms_downloader_process_child(config: dict or multiprocessing.Manager().dict,
                                  logging_queue: multiprocessing.Queue,
                                  login: str,
                                  password: str,
                                  link: str,
                                  out_dir: str,
                                  return_queue: multiprocessing.Queue) -> None:
    # Initialize logging
    LoggingHandler.worker_configurer(logging_queue)

    # Initialize class
    lms_downloader = LMSDownloader.LMSDownloader(login, password, link,
                                                 login_link=config["login_link"],
                                                 wait_between_pages=config["wait_between_pages"],
                                                 link_check_regex=config["link_check_regex"],
                                                 headless=config["headless"])

    try:
        # Download data
        return_queue.put(lms_downloader.download(out_dir))

    # LMSDownloader error
    except Exception as e:
        return_queue.put(e)

    # External interrupt
    except (KeyboardInterrupt, SystemExit):
        return_queue.put(KeyboardInterrupt())
    return_queue.put(None)


def _lms_downloader_process(config: dict or multiprocessing.Manager().dict,
                            messages: dict or multiprocessing.Manager().dict,
                            logging_queue: multiprocessing.Queue,
                            interrupt_event: multiprocessing.Event(),
                            time_started: float,
                            login: str,
                            password: str,
                            link: str,
                            chat_id: int) -> None:
    # Setup internal logging for current process
    internal_logging_queue = multiprocessing.Queue(-1)
    LoggingHandler.worker_configurer(internal_logging_queue)

    # Internal variables
    internal_logging_thread = None
    child_process = None
    temp_dir = None
    try:
        # Check asyncio event loop
        try:
            event_loop = asyncio.get_running_loop()
        except RuntimeError:
            event_loop = asyncio.new_event_loop()

        def logs_to_message_loop(config_: dict or multiprocessing.Manager().dict,
                                 messages_: dict or multiprocessing.Manager().dict,
                                 internal_logging_queue_: multiprocessing.Queue,
                                 event_loop_: asyncio.AbstractEventLoop,
                                 time_started_: float,
                                 chat_id_: int) -> None:
            """
            Redirects logs into user message (thread loop)
            :return:
            """
            log_rows = ""
            message_id = None
            last_time_sent = time.time()
            line_n = 1

            def _send_message(message_id_: int, log_rows_: str, message_base: str = "log_message") -> int:
                """
                Sends log_rows_ as message to the user
                :param message_id_: Current message ID
                :param message_base: What message to format
                :return:
                """
                if message_base == "log_message":
                    message = messages_[message_base].format(log_rows_,
                                                             config_["process_timeout"] - (time.time() - time_started_))
                    button_abort = InlineKeyboardButton(messages_["btn_abort"],
                                                        callback_data="abort_{}".format(os.getpid()))
                    reply_markup = InlineKeyboardMarkup(BotHandler.build_menu([button_abort]))
                else:
                    message = messages_[message_base].format(log_rows_)
                    reply_markup = None

                return event_loop_.run_until_complete(BotHandler.send_safe(chat_id_,
                                                                           message,
                                                                           None,
                                                                           reply_markup=reply_markup,
                                                                           edit_message_id=message_id_,
                                                                           parse_markdown=True,
                                                                           bot_api_token=config_["bot_api_token"]))

            while True:
                log_record = None
                try:
                    # Try to get one log record (non-blocking)
                    log_record = internal_logging_queue_.get(block=False)

                    # Exit?
                    if log_record is None or type(log_record) is not logging.LogRecord:
                        # Wait some time to prevent telegram errors
                        time.sleep(config_["send_messages_interval"])

                        # Normal exit (Done)
                        if log_record is None:
                            _send_message(message_id, log_rows, "log_message_done")
                        # Interrupt
                        elif type(log_record) is KeyboardInterrupt:
                            _send_message(message_id, log_rows, "log_message_done_interrupted")
                        # Error
                        else:
                            _send_message(message_id, log_rows, "log_message_done_error")

                        break
                except queue.Empty:
                    pass

                # Put into main logging queue
                if log_record is not None:
                    logging_queue.put(log_record)

                    # Ignore python-telegram-bot logs
                    if str(log_record.message).startswith(LoggingHandler.TELEGRAM_LOGS_IGNORE_PREFIX):
                        continue

                    # Append log message
                    log_rows += messages_["log_message_format"].format(line_n=line_n,
                                                                       log_entry=str(log_record.message))
                    line_n += 1

                    # Prevent overflow
                    if len(log_rows) > config_["max_log_symbols"]:
                        log_rows = log_rows[-config_["max_log_symbols"]:]

                # Send message as soon as we can
                if time.time() - last_time_sent >= config_["send_messages_interval"]:
                    last_time_sent = time.time()
                    message_id = _send_message(message_id, log_rows)

                # Non-blocking log reading
                time.sleep(.1)

        # Start it as thread
        logging.info("Starting internal logging handler")
        internal_logging_thread = threading.Thread(target=logs_to_message_loop,
                                                   args=(config,
                                                         messages,
                                                         internal_logging_queue,
                                                         event_loop,
                                                         time_started,
                                                         chat_id))
        internal_logging_thread.start()

        # Generate temp folder for storing results
        temp_dir = tempfile.TemporaryDirectory()

        # Initialize and start child process
        logging.info("Starting child process")
        return_queue = multiprocessing.Queue(-1)
        child_process = multiprocessing.Process(target=_lms_downloader_process_child,
                                                args=(config,
                                                      internal_logging_queue,
                                                      login,
                                                      password,
                                                      link,
                                                      temp_dir.name,
                                                      return_queue,),
                                                daemon=True)
        child_process.start()

        # Catch interrupt event or return data from child process
        logging.info("Waiting for data from child process or for interrupt event")
        while True:
            # Check return data
            try:
                # Try to get data from child process
                return_data = return_queue.get(block=False)

                # No data
                if return_data is None:
                    raise Exception("Child process exited but doesn't return any data")

                # Interrupt
                elif type(return_data) is KeyboardInterrupt:
                    raise KeyboardInterrupt("Keyboard interrupt from child process")

                # Error
                elif type(return_data) is Exception:
                    raise return_data

                # Seems ok
                elif type(return_data) is list:
                    # Send all files
                    for file in return_data:
                        logging.info("Sending {}".format(os.path.basename(file)))
                        retries_count = 0
                        while True:
                            retries_count += 1
                            try:
                                time.sleep(config["send_messages_interval"])
                                BotHandler.async_helper(telegram.Bot(config["bot_api_token"])
                                                        .send_document(chat_id,
                                                                       file,
                                                                       read_timeout=FILE_SEND_TIMEOUTS,
                                                                       write_timeout=FILE_SEND_TIMEOUTS,
                                                                       connect_timeout=FILE_SEND_TIMEOUTS,
                                                                       pool_timeout=FILE_SEND_TIMEOUTS))
                                break
                            except Exception as e:
                                logging.warning("Error sending file: {}".format(e))
                                logging.info("Retying after {:.1f}s. Retries: {}".format(RESEND_FILE_AFTER_TIME,
                                                                                         retries_count))
                                time.sleep(RESEND_FILE_AFTER_TIME)
                            if retries_count >= MAX_FILES_RETRIES:
                                logging.error("Max retries reached!")
                                break

                    # Done
                    logging.info("Finished in {:.2f} seconds".format(time.time() - time_started))
                    break

                # Wrong data type (or error)
                else:
                    raise Exception("Child process exited but returning wrong type of data: {}"
                                    .format(str(type(return_data))))
            # No data yet
            except queue.Empty:
                pass

            # Check interrupt event
            if interrupt_event.is_set():
                raise KeyboardInterrupt()

            # Prevent overloading
            time.sleep(.1)
    # Interrupted
    except (KeyboardInterrupt, SystemExit):
        # Terminate child process
        if child_process:
            child_process_terminate_timer = time.time()
            try:
                logging.info("Terminating child process")
                # child_process.terminate()
                os.kill(child_process.pid, signal.SIGINT)

                while True:
                    # Try to terminate child process
                    if not child_process.is_alive():
                        logging.info("Child process terminated")
                        break

                    # We have no more time to wait
                    elif time.time() - child_process_terminate_timer > CHILD_PROCESS_MAX_TERMINATE_TIME:
                        logging.warning(
                            "{:.1f}s exceeded. Killing the process".format(CHILD_PROCESS_MAX_TERMINATE_TIME))
                        child_process.kill()
                        break
            except Exception as e:
                logging.error("Error terminating child process: {}".format(str(e)))

        # Exit current process
        logging.error("Parent process was interrupted!")
        internal_logging_queue.put(KeyboardInterrupt())

    # Error occurred
    except Exception as e:
        logging.error("Error: {}".format(str(e)), exc_info=e)
        internal_logging_queue.put(e)

    # Stop internal logging
    internal_logging_queue.put(None)
    try:
        if internal_logging_thread:
            internal_logging_thread.join(timeout=10)
    except:
        pass

    # Cleaning temp dir
    if temp_dir:
        temp_dir.cleanup()


class LMSDownloaderHandler:
    def __init__(self, config: dict or multiprocessing.Manager().dict,
                 messages: dict or multiprocessing.Manager().dict,
                 logging_handler: LoggingHandler.LoggingHandler) -> None:
        self._config = config
        self._messages = messages
        self._logging_handler = logging_handler

        # (time started, process object, interrupt event)
        self.lms_downloader_processes = []
        self._processes_watchdog_thread = None
        self._processes_watchdog_enabled = False

    def processes_watchdog_start(self) -> None:
        """
        Starts processes watchdog thread
        :return:
        """
        logging.info("Starting processes watchdog thread")
        self._processes_watchdog_enabled = True
        self._processes_watchdog_thread = threading.Thread(target=self._processes_watchdog_loop)
        self._processes_watchdog_thread.start()

    def processes_watchdog_stop(self) -> None:
        """
        Tries to stop processes watchdog thread
        :return:
        """
        self._processes_watchdog_enabled = False
        if self._processes_watchdog_thread:
            logging.info("Stopping processes watchdog thread")
            try:
                self._processes_watchdog_thread.join()
                self._processes_watchdog_thread = None
            except Exception as e:
                logging.warning("Error stopping processes watchdog thread: {}".format(str(e)))
        else:
            logging.info("Processes watchdog thread already stopped")

    def interrupt_process(self, pid: int) -> None:
        """
        Sends interrupt event to the process
        :param pid: Process's PID
        :return:
        """
        for time_started, process, interrupt_event in self.lms_downloader_processes:
            if process.is_alive() and process.pid == pid:
                logging.info("Sending interrupt event")
                interrupt_event.set()
                break

    def _processes_watchdog_loop(self) -> None:
        """
        Processes watchdog thread loop
        :return:
        """
        exit_timer = 0
        while True:
            # Check each process
            for time_started, process, interrupt_event in self.lms_downloader_processes:
                # Check if process finished -> remove from list
                if not process.is_alive():
                    logging.info("Process {} finished".format(process.pid))
                    self.lms_downloader_processes.remove((time_started, process, interrupt_event))
                # Process not finished. Check timeout
                else:
                    if time.time() - time_started >= self._config["process_timeout"]:
                        # Set interrupt flag
                        if not interrupt_event.is_set():
                            logging.warning("Process timed out! Sending interrupt event and waiting {:.1f} to finish"
                                            .format(EXIT_WAIT_TIME))
                            interrupt_event.set()

                        # We don't have any more time to wait for process to finish safely, so, kill it
                        if time.time() - time_started >= self._config["process_timeout"] + EXIT_WAIT_TIME:
                            logging.warning("Process timed out! Killing it")
                            try:
                                process.kill()
                            except Exception as e:
                                logging.warning("Error killing process {}: {}".format(str(process.pid), str(e)))

            # Exit requested
            if not self._processes_watchdog_enabled:
                # But some processes are still running
                if len(self.lms_downloader_processes) > 0:
                    if exit_timer == 0:
                        # Start timer
                        exit_timer = time.time()

                        # Send interrupts
                        logging.warning("Some processes are running. Sending interrupt events and "
                                        "waiting {:.1f}s to finish".format(EXIT_WAIT_TIME))
                        for _, _, interrupt_event in self.lms_downloader_processes:
                            interrupt_event.set()

                    # We don't have any more time to wait
                    if time.time() - exit_timer > EXIT_WAIT_TIME:
                        logging.warning("Some processes not finished! Killing them")
                        for _, process, _ in self.lms_downloader_processes:
                            try:
                                process.kill()
                            except Exception as e:
                                logging.warning("Error killing process {}: {}".format(str(process.pid), str(e)))
                        break

                # No process, just exit
                else:
                    break

            # Check every 100ms
            time.sleep(0.1)

        # Log on exit
        logging.info("Processes watchdog thread finished")

    def start_new_process(self, login: str, password: str, link: str, chat_id: int) -> None:
        # Interrupt event for process
        interrupt_event = multiprocessing.Event()

        # Create process
        time_started = time.time()
        lms_downloader_process = multiprocessing.Process(target=_lms_downloader_process,
                                                         args=(self._config,
                                                               self._messages,
                                                               self._logging_handler.queue,
                                                               interrupt_event,
                                                               time_started,
                                                               login,
                                                               password,
                                                               link,
                                                               chat_id,),
                                                         daemon=False)

        # Start process
        lms_downloader_process.start()

        # Append started time, process and interrupt event to list
        self.lms_downloader_processes.append((time_started, lms_downloader_process, interrupt_event))
