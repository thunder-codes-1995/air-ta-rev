import logging
import os
import time
from typing import List

import bson as bson
import requests
from dataclasses_serialization.bson import BSONSerializer
from dotenv import load_dotenv
from jobs.lib.utils.logger import setup_logging
from me_models.hitit import Cabin, Leg

from job_queue import AuthorizationCalculationTask, Job, QueueManager, create_consumer_id
from job_queue.utils import add_randomness
from share.core.errors import auth_calc_errors as errors
from share.core.mongo.me_connect import MEConnect
from share.me_models import BreRulesResults, Hitit
from share.me_models.auth_results import AuthorizationResults, AuthorizationValue
from share.me_models.auth_results import CabinParams as CabinParamsModel
from share.typs.hitit import CalculateAvailabilityParams, FlightParams

load_dotenv()
setup_logging()
logger = logging.getLogger("fares_etl")


class AuthorizationCalculator:
    """
    Takes in flight parameters. Finds all business rules for that flight.
    Finds cabin data from hitit collection for each business rule.
    Calculates authorizations. Inserts the results to db.
    """

    ACTION_TYPES = ["UPSELL", "UNDERCUT", "DOWNSELL"]
    ACTION_RANKS = ["CLOSEST_CLASS_1", "CLOSEST_CLASS_2"]
    CABIN_MAPPING = {
        "FIRST": "F",
        "BUSINESS": "C",
        "PREMIUM": "W",
        "ECONOMY": "Y",
    }
    ACTION_TYPE_MAPPING = {
        "UPSELL": 1,
        "UNDERCUT": -1,
        "DOWNSELL": -1,
    }
    ACTION_RANK_MAPPING = {
        "CLOSEST_CLASS_1": 1,
        "CLOSEST_CLASS_2": 2,
    }

    def __init__(
        self,
        flight_params: FlightParams,
        calc_params: CalculateAvailabilityParams,
        rule_result_id: bson.ObjectId,
        connect_db: bool = True,
    ):
        if connect_db:
            MEConnect().connect()
        self.flight_params: FlightParams = flight_params
        self.calc_params: CalculateAvailabilityParams = calc_params
        self.rule_result_id = rule_result_id

    def _setup(self):
        self.flight = self._get_flight()
        self.cabins = self._get_target_cabins()

    def _get_flight(self):
        """Get the flight for which the business rules will apply"""
        flights = Hitit.objects(
            airline_code=self.flight_params.airline_code,
            origin=self.flight_params.origin,
            destination=self.flight_params.destination,
            departure_date=self.flight_params.departure_date,
            flight_number=str(self.flight_params.flight_number),
        )
        if flights.count() < 1:
            raise errors.NoFlightFoundForBusinessRuleError()
        return flights.first()

    def _get_target_cabins(self):
        """Get cabins from db for which business rules will apply"""
        cabins = []
        for leg in self.flight.legs:
            cabin_code = self.CABIN_MAPPING[self.calc_params.cabin_name]
            cabin = leg.cabins.filter(code=cabin_code).first()
            if not cabin:
                raise errors.NoCabinFoundForBusinessRuleError()
            cabins.append(cabin)
        return cabins

    def generate_authorizations(self):
        """Calculate authorizations and insert them to db"""
        self._setup()
        for leg in self.flight.legs:
            act_type = self.calc_params.action_type
            act_rank = self.calc_params.class_rank
            set_avail = self.calc_params.set_avail
            if act_type not in self.ACTION_TYPES:
                raise errors.UnknownActionTypeError(f"{act_type}")
            if act_rank not in self.ACTION_RANKS:
                raise errors.UnknownActionRankError(f"{act_rank}")

            cabin_code = self.CABIN_MAPPING[self.calc_params.cabin_name]
            cabin = leg.cabins.filter(code=cabin_code).first()
            if not cabin:
                raise errors.NoCabinFoundForBusinessRuleError()
            auth = self._calculate_auth(cabin, set_avail, act_type, act_rank)
            self._store_authorization_results(self.flight, leg, cabin, auth)
            self._update_rule_results(self.rule_result_id, auth)

    def _store_authorization_results(self, flight: Hitit, leg: Leg, cabin: Cabin, auth):
        """Persists authorization results to db"""
        auth_value_obj = AuthorizationValue(authorization=auth["authorization"], class_code=auth["class_code"], rank=auth["rank"])

        old_auth_value_obj = AuthorizationValue(
            authorization=auth["old_authorization"], class_code=auth["old_class_code"], rank=auth["old_rank"]
        )

        params_obj = CabinParamsModel(
            airline_code=flight.airline_code,
            origin=flight.origin,
            destination=flight.destination,
            departure_date=self.flight.departure_date,
            leg_origin=leg.origin,
            leg_destination=leg.destination,
            flight_number=leg.flight_number,
            cabin_code=cabin.code,
        )
        auth_obj = AuthorizationResults(
            authorization_value=auth_value_obj,
            old_authorization_value=old_auth_value_obj,
            cabin_params=params_obj,
        )

        AuthorizationResults.objects.insert([auth_obj])

    def _update_rule_results(self, rule_result_id, auth):
        """Update BRE rule results record with result of authorization calculation so that users can validate it in the UI too"""
        logger.debug(
            f"Update rule results with id: {rule_result_id} with class:{auth['class_code']} and avail:{auth['authorization']}"
        )
        BreRulesResults.objects(id=rule_result_id).update(
            __raw__={
                "$set": {
                    "actionResults.effectiveClass": auth["class_code"],
                    "actionResults.effectiveAvailability": auth["authorization"],
                }
            }
        )

    def _get_total_bookings(self, cabin):
        return sum((c.total_booking for c in cabin.classes))

    def _handle_class_code(self, cabin, step):
        target_ix = self._find_current_class_ix(cabin)
        try:
            old_cls_code = cabin.classes[target_ix].code
            target_cls_code = cabin.classes[target_ix - step].code
        except IndexError:
            raise errors.ActionRankOutOfRangeError()
        return {
            "old_class_code": old_cls_code,
            "class_code": target_cls_code,
            "old_rank": target_ix + 1,
            "rank": (target_ix - step) + 1,
        }

    def _find_current_class_ix(self, cabin):
        for i in reversed(range(len(cabin.classes))):
            if cabin.classes[i].seats_available > 0:
                return i
        raise errors.NoAvailableClassError()

    def _get_step(self, action_type, action_rank):
        sign = self.ACTION_TYPE_MAPPING[action_type]
        rank = self.ACTION_RANK_MAPPING[action_rank]
        return rank * sign

    def _calculate_auth(self, cabin, set_avail, action_type, action_rank):
        """Calculate authorizations"""
        step = self._get_step(action_type, action_rank)
        class_res = self._handle_class_code(cabin, step)
        auth = self._get_total_bookings(cabin)

        return {
            "class_code": class_res["class_code"],
            "authorization": auth + set_avail,
            "old_class_code": class_res["old_class_code"],
            "old_authorization": auth,
            "old_rank": class_res["old_rank"],
            "rank": class_res["rank"],
        }


def perform_job(qm: QueueManager, job: Job):
    job_details: AuthorizationCalculationTask = BSONSerializer.deserialize(AuthorizationCalculationTask, job.payload)
    logger.debug(f"Raw job payload:{job_details}")
    host_carrier_code = job_details.host_carrier_code
    if host_carrier_code == "PY":
        # temporarily hardcoded availability caclulation for PY only
        origin = job_details.origin
        destination = job_details.destination
        departure_date = job_details.departure_date
        logger.debug(f"Will calculate authorization at host:{host_carrier_code}, market: {origin}-{destination} {departure_date}")

        departure_date = int(departure_date.replace("-", ""))  # Date is stored in int format in mongo

        flight_params = FlightParams(
            airline_code=job_details.carrier_code,
            origin=origin,
            destination=destination,
            departure_date=departure_date,
            flight_number=job_details.flight_number,
        )

        calc_params = CalculateAvailabilityParams(
            cabin_name=job_details.cabin_name,
            action_type=job_details.action_type,
            class_rank=job_details.class_rank,
            set_avail=job_details.set_avail,
        )

        logger.debug(f"Calculating authorization")
        try:
            AuthorizationCalculator(flight_params, calc_params, job_details.rule_result_id).generate_authorizations()
        except errors.NoBusinessRuleFoundError:
            logger.error(f"No business rule found for flight: {flight_params}")
            pass
        except Exception as e:
            logger.error(f"Error while calculating authorization for flight: {flight_params}, exception: {e}")

        logger.debug(f"Authorization calculation completed succesfully, completing job: {job.job_id}")
    qm.complete_job(job)


def main():
    QUEUE_POLL_INTERVAL_SECS = int(os.getenv("REOPT_QUEUE_POLL_INTERVAL_SECS", 5))
    CONSUMER_ID = create_consumer_id("calculate_auth")
    logger.info(
        "Starting AVAILABILITY CALC consumer with id: %s, queue poll interval: %s (secs)", CONSUMER_ID, QUEUE_POLL_INTERVAL_SECS
    )
    qm = QueueManager(CONSUMER_ID)

    while True:
        job = qm.get_next_auth_calc_job()
        if job is None:
            logger.info("No new job found, sleeping")

        else:
            logger.info("Got new job: %s", job.job_id)
            try:
                perform_job(qm, job)
            except Exception as e:
                logger.error("Job failed, exception: %s", str(e))
                qm.fail_job(job, f"Exception while performing job, exception: {str(e)}")

        time.sleep(add_randomness(QUEUE_POLL_INTERVAL_SECS, 3))


if __name__ == "__main__":
    main()
