from dataclasses import dataclass
from typing import Any, Callable, List, Literal


@dataclass
class LogicOperator:
    funcs: List[Callable]
    operator: Literal["or", "and"]

    def __call__(self, *args: Any, **kwds: Any) -> bool:
        if self.operator == "or":
            return any(fun() for fun in self.funcs)
        return all(fun() for fun in self.funcs)


@dataclass
class And:
    funcs: List[Callable]

    def __call__(self, *args: Any, **kwds: Any) -> bool:
        return LogicOperator(self.funcs, "and")()


@dataclass
class Or:
    funcs: List[Callable]

    def __call__(self, *args: Any, **kwds: Any) -> bool:
        return LogicOperator(self.funcs, "or")()
