from dataclasses import dataclass
from datetime import datetime
from typing import  Optional

from bson.objectid import ObjectId
from flask import Request

from rules.events.crud.forms import CreateEventRuleForm
from rules.events.crud.generate import GenerateEventRule, GenerateEventRuleOption
from rules.repository import RuleRepository

rule_repo = RuleRepository()


@dataclass
class EventRule:
    request: Request
    form: Optional[CreateEventRuleForm] = None

    def create(self):
        item = GenerateEventRule(self.form, self.request.user).generate()
        rule_repo.insert([item])
        del item["_id"]
        return item

    def _check_rule_owner(self, rule_id, username):
        existing_rule = rule_repo.find_one(
            {"_id": ObjectId(rule_id), "username": username})
        if existing_rule is None:
            raise ValueError("You are not allowed to perform this action on this rule.")
        return existing_rule

    def update(self):
        rule = self._check_rule_owner(self.form.id, self.request.user.username)
        item = GenerateEventRule(self.form, self.request.user).generate()

        if rule['conditions']['all'][4]['all'][0]['value'] != self.form.start_date:
            if self.form.start_date < int(datetime.now().strftime("%Y%m%d")):
                raise ValueError("start_date must be later than today's date")

        if not self.form.id:
            raise ValueError("ID must be provided")
        rule_repo.update_one({"_id": ObjectId(self.form.id)}, item)
        return item

    def delete(self, _id):
        self._check_rule_owner(_id, self.request.user.username)
        rule_repo.delete({"_id": ObjectId(_id)})
        return {"message": "OK"}

    def options(self):
        return GenerateEventRuleOption(self.request).generate()