from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from bson import ObjectId
from flask import Request

from rules.flights.crud.forms import CreateFlightRuleForm
from rules.flights.crud.generate import GenerateFlightRule, GenerateFlightRuleOption
from rules.repository import RuleRepository

rule_repo = RuleRepository()


@dataclass
class FlightRule:
    request: Request
    form: Optional[CreateFlightRuleForm] = None

    def _check_rule_owner(self, rule_id, username):
        existing_rule = rule_repo.find_one(
            {"_id": ObjectId(rule_id), "username": username})
        if existing_rule is None:
            raise ValueError("You are not allowed to perform this action on this rule.")
        return existing_rule

    def create(self):
        item = GenerateFlightRule(self.form, self.request.user).generate()
        item["created_at"] = datetime.utcnow()
        item["updated_at"] = datetime.utcnow()
        rule_repo.insert([item])
        del item["_id"], item["created_at"], item["updated_at"]
        return item

    def update(self):
        self._check_rule_owner(self.form.id, self.request.user.username)
        item = GenerateFlightRule(self.form, self.request.user).generate()

        if not self.form.id:
            raise ValueError("ID must be provided")

        item["updated_at"] = datetime.utcnow()
        rule_repo.update_one({"_id": ObjectId(self.form.id)}, item)

        del item["updated_at"]
        return item

    def delete(self, _id):
        self._check_rule_owner(_id, self.request.user.username)
        rule_repo.delete({"_id": ObjectId(_id)})
        return {"message": "OK"}

    def options(self):
        return GenerateFlightRuleOption(self.request).generate()