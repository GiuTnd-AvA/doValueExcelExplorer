import unittest
from Connection.Get_Xml_Connection import GetXmlConnection

class TestDbPriority(unittest.TestCase):
    def setUp(self):
        self.g = GetXmlConnection('dummy.xlsx')

    def decide(self, conn_db, cmd_db):
        chosen_db = conn_db
        if cmd_db:
            if not conn_db:
                chosen_db = cmd_db
            else:
                c_low = conn_db.strip().lower()
                q_low = cmd_db.strip().lower()
                if c_low == 'analisi' or c_low.startswith('analisi_'):
                    chosen_db = cmd_db
                elif c_low != q_low:
                    chosen_db = cmd_db
        return chosen_db

    def test_use_statement(self):
        cmd = 'USE s1057b; SELECT * FROM dbo.TableX'
        db, schema, table = self.g._parse_command(cmd)
        self.assertEqual(db, 's1057b')

    def test_from_qualified(self):
        cmd = 'SELECT * FROM s1057b.dbo.TableX'
        db, schema, table = self.g._parse_command(cmd)
        self.assertEqual(db, 's1057b')
        self.assertEqual(schema, 'dbo')
        self.assertEqual(table, 'TableX')

    def test_bracketed_names(self):
        cmd = 'SELECT * FROM s1057b.dbo.[SVG_DA CAMPIONARE]'
        db, schema, table = self.g._parse_command(cmd)
        self.assertEqual(db, 's1057b')
        self.assertEqual(schema, 'dbo')
        self.assertEqual(table, 'SVG_DA CAMPIONARE')

    def test_priority_when_conn_is_analisi(self):
        conn_db = 'ANALISI'
        cmd_db = 's1057b'
        chosen = self.decide(conn_db, cmd_db)
        self.assertEqual(chosen, 's1057b')

    def test_priority_when_mismatch(self):
        conn_db = 's1057b'
        cmd_db = 's1057'
        chosen = self.decide(conn_db, cmd_db)
        self.assertEqual(chosen, 's1057')

    def test_fallback_to_conn(self):
        conn_db = 's1057b'
        cmd_db = None
        chosen = self.decide(conn_db, cmd_db)
        self.assertEqual(chosen, 's1057b')

if __name__ == '__main__':
    unittest.main()
