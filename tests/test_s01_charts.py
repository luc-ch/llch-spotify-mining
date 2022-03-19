import unittest
from IPython.display import display

from spotify_mining.mining import s01_extract_charts
from spotify_mining.configurations import Configuration


class TestCharts(unittest.TestCase):
    def test_charts(self):
        Configuration("config.ini")
        df = s01_extract_charts.ExtractCharts().get_charts_in_dataframe(
            date="2020-04-17", region="ar"
        )
        display(df.head(10))
        assert len(df) == 200


if __name__ == "__main__":
    unittest.main()
