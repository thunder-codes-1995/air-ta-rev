import json
from bson import json_util
from base.service import BaseService
from fare_analyzer.repository import Fares2Repository
from fare_analyzer.query import FareQuery

class FareAnalyzerService(BaseService):
    repository_class = Fares2Repository

    def get_all_fares(self):
        pipelines = FareQuery().query()
        results = self.repository.aggregate(pipelines)

        season_mapping = {
            "L": "LOW",
            "O": "SHOULDER",
            "H": "HIGH",
            "J": "Basic",
            "P": "Peak",
            "Q": "Holiday",
            "Z": "winter",
        }

        formatted_results = []
        for result in results:
            season_data = {
                "season": season_mapping.get(result["season"], result["season"]),
                "lowest_fare_levels": [
                    {
                        "brand": brand.get("brand"),
                        "lowest_fare": brand.get("lowest_fare")
                    }
                    for brand in result["brands"]
                ],
                "expanded_fares": result["brands"]
            }
            formatted_results.append(season_data)

        return json.loads(json_util.dumps(formatted_results))
