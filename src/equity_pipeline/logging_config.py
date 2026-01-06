import logging


def setup_logging(handler=None, level=logging.INFO):
    root = logging.getLogger()
    root.setLevel(level)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    if handler:
        handler.setFormatter(formatter)
        root.addHandler(handler)
    else:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root.addHandler(console)
