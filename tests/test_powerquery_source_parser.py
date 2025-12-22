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

    def test_parse_temp_table_double_hash(self):
        src = (
            'Source = Sql.Database("EPCP3", "ANALISI", '
            '[Query="select * from ##PIVOT_BUDGET#(lf)"])'
        )
        info = self.parser.parse(src)
        self.assertEqual(info["server"].lower(), "epcp3")
        # DB not specified in FROM, fallback to parameter
        self.assertEqual(info["database"].lower(), "analisi")
        self.assertEqual(info["schema"].lower(), "temp")
        self.assertEqual(info["table"], "PIVOT_BUDGET")

    def test_parse_temp_table_with_tab_token(self):
        src = (
            'Source = Sql.Database("epcp3", "analisi", '
            '[Query="SELECT * FROM #(tab)##PIVOT_BUDGET_FEE"])'
        )
        info = self.parser.parse(src)
        self.assertEqual(info["schema"].lower(), "temp")
        self.assertEqual(info["table"], "PIVOT_BUDGET_FEE")

    def test_parse_all_with_join_and_db_schema(self):
        src = (
            'Source = Sql.Database("EPCP3", "S1057B", '
            '[Query="select * from S1057B.dbo.PM_contacting_OUTPUT as pm '
            'left join CORESQL7.[dbo].[proposte_di_delibera_tipo] pdt on pm.id=pdt.id '
            'inner join [S1057B].[dbo].[DB_TOTALE] dt on pm.x=dt.x"])'
        )
        infos = self.parser.parse_all(src)
        # Expect three tables extracted
        names = {(i.get("database"), i.get("schema"), i.get("table")) for i in infos}
        self.assertIn(("S1057B", "dbo", "PM_contacting_OUTPUT"), names)
        self.assertIn(("CORESQL7", "dbo", "proposte_di_delibera_tipo"), names)
        self.assertIn(("S1057B", "dbo", "DB_TOTALE"), names)

    def test_parse_db_dotdot_table_missing_schema(self):
        src = (
            'Source = Sql.Database("epcp3", "s1057b", '
            '[Query="select * from s1057b..ReportAste_doBank"])'
        )
        info = self.parser.parse(src)
        self.assertEqual(info["database"].lower(), "s1057b")
        self.assertIsNone(info["schema"])
        self.assertEqual(info["table"], "ReportAste_doBank")

        infos = self.parser.parse_all(src)
        self.assertTrue(any(i.get("table") == "ReportAste_doBank" and (i.get("schema") is None) for i in infos))


if __name__ == "__main__":
    unittest.main()
