from dataclasses import dataclass


@dataclass
class CabinMapper:
    mp = {"ECONOMY": "Y", "ECO": "Y", "BUSINESS": "J", "BUS": "J", "FIRST": "F"}

    @classmethod
    def normalize(cls, cabin_name: str) -> str:
        return cls.mp.get(cabin_name.upper(), cabin_name)

    @classmethod
    def humanize(cls, cabin_code: str) -> str:
        d = {v: k for k, v in cls.mp.items()}
        return d.get(cabin_code.upper(), cabin_code)
