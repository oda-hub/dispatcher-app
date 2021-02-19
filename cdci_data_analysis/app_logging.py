import logging
import logging_tree

class AppLogging:
    @property
    def level_by_logger(self) -> dict:
        return getattr(self, '_level_by_logger', {"":"info"})

    @level_by_logger.setter
    def level_by_logger(self, level_by_logger: dict):
        self._level_by_logger = level_by_logger

    def __init__(self):
        pass

    def setup(self, tree=None) -> None:
        if tree is None:
            tree = logging_tree.tree()

            if self.level_by_logger.get("", "info").upper() == "DEBUG":
                logging_tree.printout()

        for n, l in self.level_by_logger.items():
            if n == tree[0]:
                print(f"\033[33m setting logger \"{n}\" ({tree[1]}) at level {l}\033[0m")
                tree[1].setLevel(l.upper())

        for child in tree[2]:
            self.setup(child)

    def getLogger(self, *a, **aa):
        logger = logging.getLogger(*a, **aa)
        self.setup()
        return logger

app_logging = AppLogging()

