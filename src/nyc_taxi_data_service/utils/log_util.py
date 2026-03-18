import logging


class ColorFormatter(logging.Formatter):
    format_str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

    # set colors for different log levels
    RESET = "\033[0m"
    LEVEL_COLORS = {
        logging.DEBUG: "\033[90m",  # grey
        logging.INFO: "\033[32m",  # green
        logging.WARNING: "\033[33m",  # yellow
        logging.ERROR: "\033[31m",  # red
        logging.CRITICAL: "\033[91m",  # bright red
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, self.RESET)
        log_format = f"{color}{self.format_str}{self.RESET}"
        formatter = logging.Formatter(log_format)
        return formatter.format(record)


def setup_logging(level: str = "INFO") -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # clear existing handlers, if there's any
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # create a hanlder and apply color format
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter())
    root_logger.addHandler(handler)
