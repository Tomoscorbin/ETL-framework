"""Logging configuration and helpers."""

import logging
import typing
from enum import StrEnum

from src import settings


class ConsoleFormat(StrEnum):
    """Formatting options for console logging.

    <https://en.wikipedia.org/wiki/ANSI_escape_code#Select_Graphic_Rendition_parameters>
    """

    RESET = "\033[0m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    LIGHT_GREY = "\033[37m"

    HIGHLIGHT_BLACK = "\033[40m"
    HIGHLIGHT_RED = "\033[41m"
    HIGHLIGHT_GREEN = "\033[42m"
    HIGHLIGHT_YELLOW = "\033[43m"
    HIGHLIGHT_BLUE = "\033[44m"
    HIGHLIGHT_MAGENTA = "\033[45m"
    HIGHLIGHT_CYAN = "\033[46m"
    HIGHLIGHT_LIGHT_GREY = "\033[47m"

    BOLD = "\033[1m"
    LIGHT = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    HIGHLIGHT = "\033[7m"
    STRIKETHROUGH = "\033[9m"


class DefaultConsoleFormatter(logging.Formatter):
    """The default console formatter to use."""

    fmt = "{asctime} - {name} - {levelname} - {message}"
    style = "{"
    validate = True

    def _formatted_mesage(self, *_: typing.Any, **__: typing.Any) -> str:
        return self.fmt

    def format(self, record: logging.LogRecord) -> str:
        """Format the specified log record as text."""
        formatter = logging.Formatter(
            self._formatted_mesage(record),
            style=self.style,  # type: ignore[arg-type]
            validate=self.validate,
        )
        return formatter.format(record)


class ColourConsoleFormatter(DefaultConsoleFormatter):
    """A formatter that has colour support for console logging."""

    COLOURS = {
        logging.DEBUG: ConsoleFormat.LIGHT_GREY,
        logging.INFO: ConsoleFormat.BLUE,
        logging.WARNING: ConsoleFormat.YELLOW,
        logging.ERROR: ConsoleFormat.RED,
        logging.CRITICAL: ConsoleFormat.BOLD + ConsoleFormat.HIGHLIGHT_RED + ConsoleFormat.BLACK,
    }

    def _formatted_mesage(self, record: logging.LogRecord) -> str:
        log_colour = self.COLOURS.get(record.levelno, ConsoleFormat.RESET)
        return f"{log_colour}{self.fmt}{ConsoleFormat.RESET}"


_stream_handler = logging.StreamHandler()
_stream_handler.setLevel(settings.LOG_LEVEL)

if settings.LOG_COLOUR_ENABLED:  # pragma: nocover
    _stream_handler.setFormatter(ColourConsoleFormatter())
else:  # pragma: nocover
    _stream_handler.setFormatter(DefaultConsoleFormatter())


LOGGER = logging.getLogger(settings.LOGGER_NAME)
LOGGER.setLevel(settings.LOG_LEVEL)
LOGGER.addHandler(_stream_handler)
