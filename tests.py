import unittest
import unittest.mock
import base64
from get_data import tasks
import io
import datetime
from pyot.core.queue import Queue
import asyncio
import main


class MyTestCase(unittest.TestCase):

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
            expected_history = """'match_history': ['EUW1_6356542216', 'EUW1_6356497497', 'EUW1_6356065287', 'EUW1_6356027087']"""
            self.assertIn(expected_history, mock_stdout.getvalue())

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    async def test_consume_match(self, mock_stdout):
        async with Queue() as queue:
            await tasks.consume_match(self.match, print)
            self.assertIn("'match_id': 'EUW1_6359993696'", mock_stdout.getvalue())


if __name__ == '__main__':
    unittest.main()
