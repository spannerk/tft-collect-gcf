import unittest
import unittest.mock
import base64
from get_data import tasks
import io
import datetime
from pyot.core.queue import Queue
import asyncio
from google.cloud import storage
import main


class UnitTests(unittest.TestCase):

    def setUp(self):
        self.event = type('cloudevent', (object,), {"attributes": {}, "data": {}})

        self.event.data = {
            "message": {
                "data": base64.b64encode(b"Hi Hannah G"),
            }
        }

    # @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    # def test_subscribe(self, mock_stdout):
    #     main.subscribe(self.event)
    #     self.assertEqual(mock_stdout.getvalue(), "Hi Hannah G\n")

    def test_get_time_range(self):
        results = tasks.get_time_range(datetime.datetime(2023, 4, 3, 0, 0), 12, 1)
        self.assertEqual(results, (datetime.datetime(2023, 4, 2, 11, 0), datetime.datetime(2023, 4, 2, 23, 0)))

    def test_config_import(self):
        from pyot.conf.utils import import_confs
        import_confs("get_data.pyot_config")
        from pyot.models import tft
        summoner = tft.Summoner(puuid='fMGl27BNFhIgSQraQAs0FrmOAqEduNkmEoJWeZ1kEZSLrihicQER-0cHzkywxsS5eNQ-TGFA_a_V1g')
        self.assertIsNotNone(summoner)


class PyotTestCase(unittest.IsolatedAsyncioTestCase):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def setUp(self):
        from pyot.conf.utils import import_confs
        import_confs("get_data.pyot_config")
        from pyot.models import tft
        self.summoner = tft.Summoner(name="spannertft")
        self.match = tft.Match(id="EUW1_6359993696")

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    async def test_consume_summoner(self, mock_stdout):
        async with Queue() as queue:
            test_starttime, test_endtime = datetime.datetime(2023, 4, 2, 11, 0), datetime.datetime(2023, 4, 2, 23, 0)
            await tasks.consume_summoner(self.summoner, test_starttime, test_endtime, None, None)
            self.assertIn("spannertft", mock_stdout.getvalue())

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    async def test_consume_summoner_print(self, mock_stdout):
        async with Queue() as queue:
            test_starttime, test_endtime = datetime.datetime(2023, 4, 11, 11, 0), datetime.datetime(2023, 4, 11, 23, 0)
            await tasks.consume_summoner(self.summoner, test_starttime, test_endtime, print, None)
            expected_history = """["EUW1_6356542216", "EUW1_6356497497", "EUW1_6356065287", "EUW1_6356027087"]"""
            self.assertIn(expected_history, mock_stdout.getvalue())

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    async def test_consume_match(self, mock_stdout):
        async with Queue() as queue:
            await tasks.consume_match(self.match, print)
            self.assertIn('EUW1_6359993696', mock_stdout.getvalue())

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    async def test_get_summoner_matches(self, mock_stdout):
        test_starttime, test_endtime = datetime.datetime(2023, 4, 11, 11, 0), datetime.datetime(2023, 4, 11, 23, 0)
        test_puuids = ['IZnpt2d5pvurXXIOcl9aSFD5eooUnMcvjAM1C8BJ32Snp-EZflUu5pdFwzM-4YoXmLpStAu7Plf2dA',
                       'IMKXFDgs25rueiGfxVjkkMD-u97oeoUw3NBrLp61SXDrm2ZPN-gcv4qgjZHYxpaL6UiEygvfQpD65A']
        await tasks.get_summoner_matches(test_puuids, test_starttime, test_endtime, print, None)
        self.assertIn("spannertft", mock_stdout.getvalue())
        self.assertIn("SueMG", mock_stdout.getvalue())


class ProcessTestCase(unittest.IsolatedAsyncioTestCase):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def setUp(self):
        from pyot.conf.utils import import_confs
        import_confs("get_data.pyot_config")
        from pyot.models import tft
        self.summoner = tft.Summoner(name="spannertft")
        self.match = tft.Match(id="EUW1_6359993696")
        self.bucket_name = 'tft_test_' + datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")
        storage_client = storage.Client(project="verdant-wave-375715")
        self.bucket = storage_client.create_bucket(self.bucket_name)

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    async def test_process_run(self, mock_stdout):
        configs = {
            'project_name': 'verdant-wave-375715',
            'input_bucket_name': "summoner_checklist",
            'input_blob_name': "summoner_list_test.csv",
            'summoner_output_func': tasks.gcs_write,
            'summoner_output_location': self.bucket_name,
            'match_output_func': tasks.gcs_write,
            'match_output_location': self.bucket_name,
            'num_puuids': 2,
            'time_window_hours': 12,
            'time_offset_hours': 1
        }

        run_scheduled = datetime.datetime(2023, 4, 3, 0, 0, 0)

        await tasks.process_run(run_scheduled, configs)

        expected_files = [
            "IMKXFDgs25rueiGfxVjkkMD-u97oeoUw3NBrLp61SXDrm2ZPN-gcv4qgjZHYxpaL6UiEygvfQpD65A_2023-04-02 11:00:00_2023-04-02 23:00:00.json",
            "IZnpt2d5pvurXXIOcl9aSFD5eooUnMcvjAM1C8BJ32Snp-EZflUu5pdFwzM-4YoXmLpStAu7Plf2dA_2023-04-02 11:00:00_2023-04-02 23:00:00.json",
            "EUW1_6343076097.json",
            "EUW1_6343131079.json",
            "EUW1_6343372556.json",
            "EUW1_6343653546.json",
            "EUW1_6343872631.json",
            "EUW1_6343995009.json"
            ]
        expected_files.sort()

        resulting_files = [blob.name for blob in self.bucket.list_blobs()]
        resulting_files.sort()
        self.assertListEqual(expected_files, resulting_files)

    def tearDown(self):
        for blob in self.bucket.list_blobs():
            blob.delete()
        self.bucket.delete()


if __name__ == '__main__':
    unittest.main()
