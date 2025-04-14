import unittest

import pandas as pd
from __handlers.hitit import CsvHandler


class TestCSVDailyHandler(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName)
        self.handler = CsvHandler(date=20230101, path_to_files="tests/test_handlers/files/hitit")
        self.data = self.handler.parse()

    def test_flight_segments_count(self):
        """check segments count"""
        seg_df = self.handler.get_flight_segments(self.handler.segments)
        seg_df = seg_df.drop_duplicates(["seg_origin", "seg_destination"])
        self.assertEqual(seg_df.shape[0], 4)

    def test_get_flight_segments(self):
        """check segments orign and dest code"""
        seg_df = self.handler.get_flight_segments(self.handler.segments)
        seg_df = seg_df.drop_duplicates(["seg_origin", "seg_destination"])
        segments = list(zip(seg_df.seg_origin, seg_df.seg_destination))

        self.assertEqual(segments[0][0], "PBM")
        self.assertEqual(segments[0][1], "GEO")
        self.assertEqual(segments[1][0], "PBM")
        self.assertEqual(segments[1][1], "MIA")
        self.assertEqual(segments[2][0], "GEO")
        self.assertEqual(segments[2][1], "MIA")
        self.assertEqual(segments[3][0], "PBM")
        self.assertEqual(segments[3][1], "AUA")

    def test_handle_null_fields(self):
        """handler should be able to handle all null columns"""
        self.assertTrue(not all(self.handler.segments.isnull().any().tolist()))
        self.assertTrue(not all(self.handler.legs.isnull().any().tolist()))

    def test_get_segment_slice(self):
        """get start and end index for on segment's legs"""
        legs_df = pd.DataFrame(
            {
                "leg_origin": [
                    "PBM",
                    "PBM",
                    "LCA",
                    "LCA",
                    "LCA",
                    "XX",
                    "XX",
                    "GEO",
                    "GEO",
                    "XX",
                ],
                "leg_destination": [
                    "GEO",
                    "GEO",
                    "XX",
                    "XX",
                    "XX",
                    "MIA",
                    "MIA",
                    "XX",
                    "XX",
                    "TLV",
                ],
                "flt_number": [481] * 10,
                "flt_dept_date": [20231010] * 10,
            }
        )

        f_start_idx, f_end_idx = self.handler.get_segment_slice(legs_df, "PBM", "GEO")
        s_start_idx, s_end_idx = self.handler.get_segment_slice(legs_df, "LCA", "MIA")
        t_start_idx, t_end_idx = self.handler.get_segment_slice(legs_df, "GEO", "TLV")

        self.assertEqual((f_start_idx, f_end_idx), (0, 2))
        self.assertEqual((s_start_idx, s_end_idx), (2, 7))
        self.assertEqual((t_start_idx, t_end_idx), (7, 10))

    def test_get_segment_legs(self):
        pbm_geo_df = self.handler.get_segment_legs("PBM", "GEO", 421, 20221220)
        geo_mia_df = self.handler.get_segment_legs("GEO", "MIA", 421, 20221220)
        pbm_aua_df = self.handler.get_segment_legs("PBM", "AUA", 631, 20221215)

        self.assertEqual(pbm_geo_df.shape[0], 42)
        self.assertEqual(geo_mia_df.shape[0], 42)
        self.assertEqual(pbm_aua_df.shape[0], 11)

    # def test_class_order(self):
    #     """check order of classes (should be ordered ascending)"""
    #     df = self.__get_classes()
    #     for _, g_df in df.groupby("cabin"):
    #         print(g_df)
    #         self.assertIs(g_df["display_sequence"].is_monotonic_increasing, True)

    # def test_class_belong_to_cabin(self):
    #     """make sure clases belong to corresponding cabins"""
    #     df = self.__get_classes()
    #     c_cabin_df = df[df.cabin == "C"]
    #     y_cabin_df = df[df.cabin == "Y"]

    #     for class_code in ["J", "C", "I"]:
    #         self.assertIn(class_code, c_cabin_df["class"].tolist())

    #     for class_code in ["L", "H", "B", "Y", "M", "Q", "K", "S"]:
    #         self.assertIn(class_code, y_cabin_df["class"].tolist())

    #     for class_code in ["J", "C", "I"]:
    #         self.assertNotIn(class_code, y_cabin_df["class"].tolist())

    #     for class_code in ["L", "H", "B", "Y", "M", "Q", "K", "S"]:
    #         self.assertNotIn(class_code, c_cabin_df["class"].tolist())
