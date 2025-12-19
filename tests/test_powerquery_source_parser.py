import unittest

from BusinessLogic.PowerQuerySourceConnectionParser import PowerQuerySourceConnectionParser


class TestPowerQuerySourceParser(unittest.TestCase):
    def setUp(self):
        self.parser = PowerQuerySourceConnectionParser()

    def test_parse_with_db_in_query(self):
        # Example similar to screenshot: server EPCP3, db in Sql.Database is S1057B,
        # but the FROM uses S1259.dbo.A1R_MandantixPTFS -> prefer S1259
        src = (
            'Source = Sql.Database("EPCP3", "S1057B", '
            '[Query="SELECT * FROM S1259.dbo.A1R_MandantixPTFS"])'
        )
        info = self.parser.parse(src)
        self.assertEqual(info["server"].lower(), "epcp3")
        self.assertEqual(info["database"].lower(), "s1259")
        self.assertEqual(info["schema"].lower(), "dbo")
        self.assertEqual(info["table"], "A1R_MandantixPTFS")

    def test_parse_with_schema_only_in_query(self):
        # No DB in FROM, fallback to Sql.Database db param
        src = (
            'Source = Sql.Database("epcp3", "s1057", '
            '[Query="select * from dbo.RELAIS_LegalProcedure"])'
        )
        info = self.parser.parse(src)
        self.assertEqual(info["server"].lower(), "epcp3")
        self.assertEqual(info["database"].lower(), "s1057")
        self.assertEqual(info["schema"].lower(), "dbo")
        self.assertEqual(info["table"], "RELAIS_LegalProcedure")

    def test_parse_with_m_linefeeds_and_brackets(self):
        # Handles # (lf) tokens and [bracketed] identifiers
        src = (
            'Source = Sql.Database("EPCP3", "S1057B", '
            '[Query="select *#(lf)from#(lf)[S1057B].[DBO].[DB_TOTALE]"])'
        )
        info = self.parser.parse(src)
        self.assertEqual(info["server"].lower(), "epcp3")
        self.assertEqual(info["database"].lower(), "s1057b")
        self.assertEqual(info["schema"].lower(), "dbo")
        self.assertEqual(info["table"], "DB_TOTALE")


if __name__ == "__main__":
    unittest.main()
