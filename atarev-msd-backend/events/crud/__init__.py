from dataclasses import dataclass
from typing import Dict, Literal, TypedDict, Union

from bson.objectid import ObjectId
from flask import Request

from configurations.repository import ConfigurationRepository
from events.crud.action import Action
from events.repository import EventRepository

from .form import CreateEvent, CreateMultipleEvents, UpdateEvent
from .generate import CreateItemType, GenerateCreateItem, GenerateUpdateItem, UpdateItemType

event_repo = EventRepository()
config_repo = ConfigurationRepository()


class CreateResp(CreateItemType):
    id: str


class UpdateResp(UpdateItemType):
    id: str


class DeleteResp(TypedDict):
    id: str


@dataclass
class RemoveEvent:
    data: Dict[str, str]
    host_code: str

    def delete(self) -> DeleteResp:
        event_repo.delete({"_id": ObjectId(self.data["id"]), "airline_code": self.host_code})
        return {"id": self.data["id"]}


@dataclass
class StoreEvent:
    request: Request
    action: Literal["create", "update"]
    data: Dict[str, str]
    host_code: str

    def store(self) -> Union[CreateResp, UpdateResp]:
        markets = config_repo.get_customer_markets(self.request.user.carrier)["markets"]

        if self.action == Action.CREATE.value:

            # validate values
            events = CreateMultipleEvents(**self.data).data(markets=markets)
            # create dict to be stored
            create_objects = []
            for event in events:
                create_objects.append(
                    GenerateCreateItem(
                        start_date=event["start_date"],
                        end_date=event["end_date"],
                        name=event["name"],
                        category=event["category"],
                        sub_category=event["sub_category"],
                        airline_code=self.host_code,
                        city=event["city_code"],
                        country_code=event["country_code"],
                    ).generate()
                )

            # store new value
            results = event_repo.insert(create_objects)

            response = list(event_repo.find({"_id": {"$in": results.inserted_ids}}))
            for r in response:
                del r["_id"]
            return response

        else:

            # validate values
            data = UpdateEvent(**self.data).data(markets=markets)

            # create dict to be stored
            update_obj = GenerateUpdateItem(
                start_date=data["start_date"],
                end_date=data["end_date"],
                category=data["category"],
                name=data["name"],
                sub_category=data["sub_category"],
                city=data["city_code"],
                country_code=data["country_code"],
            ).generate()

            item = event_repo.find_one({"_id": ObjectId(self.data["id"]), "airline_code": self.host_code})
            assert bool(item), "Event not found"

            # store new value
            event_repo.update_one(
                {"_id": ObjectId(self.data["id"]), "airline_code": self.host_code},
                update_obj,
            )

            return {**update_obj, "id": self.data["id"]}
