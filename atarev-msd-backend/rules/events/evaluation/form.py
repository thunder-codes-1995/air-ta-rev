from pydantic import BaseModel


class AlertEvaluationForm(BaseModel):
    host_code: str
