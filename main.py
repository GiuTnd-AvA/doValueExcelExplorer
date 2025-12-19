from Config.config import POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH, EXCEL_OUTPUT_PATH
from PowerShellScripts.Excecute_Power_Shell_Script import ExecPsCode as ps
from BusinessLogic.Business_Logic import BusinessLogic as bl
from Report.Excel_Writer import ExcelWriter as ew

run_ps = ps(POWERSHELL_SCRIPT_PATH, EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)
return_code, output, error = run_ps.run()

if return_code == 0:
    print("PowerShell script executed successfully.")
    print("Output:")
    print(output)
else:
    print("PowerShell script execution failed.")
    print("Error:")
    print(error)

bl_obj = bl(EXCEL_ROOT_PATH, EXPORT_MCODE_PATH)

# Definizione colonne per export file e connessioni
columns_file_list = ['Percorsi', 'File']
columns_connessioni = ['File_Name',
                       'Creatore_file',
                       'Ultimo_modificatore_file',
                       'Data_creazione_file',
                       'Data_ultima_modifica_file',
                       'Collegamento_esterno',
                       'Source',
                       'Server',
                       'Database',
                       'Schema',
                       'Table',
                       'Join',
                       'Type',
                       'N_Connessioni_PQ']

excel_files_list = bl_obj.split_excel_root_path()
aggregated_info = bl_obj.get_aggregated_info()

excel_files_list = bl_obj.split_excel_root_path()
     # ...existing code...

stampa_report_connessioni = ew(rf'C:\Users\ciro.andreano\Desktop','Report_Connessioni.xlsx')
stampa_report_connessioni.write_excel(columns_file_list, excel_files_list, sheet_name = 'Lista file')

# Quando si costruisce la lista per l'export, associa ogni .txt al relativo .xlsx
# Supponendo che aggregated_info sia una lista di record dove il primo campo è il nome file .txt
# e che tu voglia sostituire solo l'estensione per l'export


# Corretto: associa ogni .txt al relativo .xlsx mantenendo il nome completo (non solo la radice)
def convert_txt_to_xlsx(records):
    new_records = []
    for row in records:
        if isinstance(row, (list, tuple)) and row:
            file_txt = row[0]
            # Sostituisci solo l'estensione .txt con .xlsx, mantenendo il nome completo
            if file_txt.lower().endswith('.txt'):
                file_xlsx = file_txt[:-4] + '.xlsx'
                new_row = (file_xlsx,) + tuple(row[1:])
                new_records.append(new_row)
            else:
                new_records.append(row)
        else:
            new_records.append(row)
    return new_records


# Filtra i duplicati per nome file (prima colonna)
def filter_duplicates(records):
    seen = set()
    filtered = []
    for row in records:
        if row and row[0] not in seen:
            filtered.append(row)
            seen.add(row[0])
    return filtered

# Prima di esportare in Excel:
aggregated_info_export = convert_txt_to_xlsx(aggregated_info)
aggregated_info_export = filter_duplicates(aggregated_info_export)
stampa_report_connessioni.write_excel(columns_connessioni, aggregated_info_export, sheet_name = 'Connessioni')
print("Report connessioni creato correttamente.")

# --- Confronto connessioni attese vs estratte (stampa tabellare) ---
from collections import Counter


# Lista fornita dall'utente: filename\tn_connessioni_attese (estensione .txt)
user_counts = '''
DB_Contatti_v8_x_KPI.txt\t95
Delibere_Master_Report_Team_V12.txt\t55
Report Delibere_KPI_2025_split_noGenn_v38_SG.txt\t17
Report Contatti_KPI_2025_split_noGenn_v22_SG.txt\t17
Semiannual Report ICCREA 3_20250630_v1.txt\t12
Quarterly Report ICCREA 3_20250630_v1.txt\t12
BCC_NPLs_2018-2_Semiannual_Report_20250630_v1.txt\t12
05.Notule_Perimetri_da_Controllare_Parte_1^_MASTER_v3.txt\t12
BCC_NPLs_2018-2_Quarterly_Report_20250630_v1.txt\t12
Reporting_RELAIS.txt\t10
Luzz23_Tabelle_Sem_202505.txt\t10
Luzz23_Tabelle_Trim_202505.txt\t10
06.Notule_Perimetri_da_Controllare_Parte_2^_MASTER_v3.txt\t10
BCC_NPLs_2018-2_Monthly_Report_20250531.txt\t10
Weekly_Report_Master_New.txt\t9
Delibere_%Respinte_Master_2025_macro_v1.txt\t9
ISEO_Tabelle_Sem_202506.txt\t9
Olympia - Quarterly_Report_Master_draft.txt\t8
AUM_IT_202503_PB.txt\t8
01_GESTITO 20241231 - PWC.txt\t8
ISEO_Tabelle_Trim_202506.txt\t8
Carichi_x_gestore_working_2025_YTD.txt\t8
UCL_cause_passive_master.txt\t6
Toulosa_IPD20250430_Verifica Calcolo_Fee_Commissioni_ReportSpese.txt\t6
work_rec_v00_PB.txt\t6
Olympia - Incassi riclassificati EPC.txt\t6
Itaca - Quarterly_Report_Master_draft_2025_03_31.txt\t5
003_DB_MASTER_PE.txt\t5
Semiannual Report BCC NPLs 2021_20250331.txt\t4
Quarterly Report BCC NPLs 2021_20250630.txt\t4
pratiche alla rete_perimetro_2025_working.txt\t4
20250702 Report Spese di Recupero Luzzatti_work 2.txt\t4
Check Commissioni_Giu25.txt\t4
PwC_IFAMS_Working_v3_no_esino efesto_202412.txt\t3
Report_Aste_Master_v4.txt\t3
Quarterly Servicing Report_Leasing_New.txt\t3
Olympia - SRDR_SADR_WORK_2024_12_31_INTEGRAZIONE.txt\t3
Ortles - Quarterly_Report_Master_draft.txt\t3
Portafoglio as of 2024 12 31_pivot per cartina italia.txt\t3
IT_CS_CF_202505.txt\t3
Immobili.txt\t3
File_Master_w_query.txt\t3
03_CHIUSURE E INCASSI 20241231 - PWC.txt\t3
Incassi EPC Ortles 21_Master.txt\t3
Carichi_x_AFC_Master.txt\t3
doRE_Valuation_Requests_MASTER.txt\t3
Aggiornamento piano di rientro_2025_07_15.txt\t3
g1_e_g2_Master.txt\t3
Check Commissioni 25Q1.txt\t3
Immobili-Garanzie_20250531.txt\t3
Crediveneto - chiusure.txt\t3
auto_luck.txt\t3
db bad0rapporti post flusso20250618.txt\t3
BCC_2018-2_Monitoraggio Spese_work_v1_al_31052025.txt\t3
SAL_S025_2025_SISTEMAZIONE CASH IN COURT.txt\t2
NPL Performance Report_Asset_v4.txt\t2
Quarterly Servicing Report_Loans_New.txt\t2
Monitoring_sla_UCI - master.txt\t2
NPV RATIO_work.txt\t2
Padovana_35247_SOF_DCR_311224.txt\t2
Monitoraggio Spese_ICCREA 5_work_30042025.txt\t2
PERIMETRO_PilotaSmall_20250625_doV_PilotaTeam4.txt\t2
Stato_repossessed_work.txt\t2
Report_AsteDaRifissare_master_v3.txt\t2
SVG Aggiornamento 20250620.txt\t2
SAL_Rendicontazioni_OLYMPIA_V3_pivot.txt\t2
Previsioni_EPC_New - Adj Bazzarelli.txt\t2
Spese EPC_20250331.txt\t2
New_profitability_detail_FIG_Q12025_Work.txt\t2
New_profitability_detail_FundIII_Q125.txt\t2
Master_NewRpt_Auctions.txt\t2
Rate arretrate_consorzio_UCI_primacasa.txt\t2
Collection Type - Tipologia Incassi.txt\t2
IRPINA_35247_SOF_DCR_311224.txt\t2
BCC_Npls_19_Loan by Loan_202506_work.txt\t2
GARANTIMag202506191030.txt\t2
Analisi_Resolved_202503.txt\t2
Conflitto di interesse_20250223.txt\t2
01_GESTITO.txt\t2
COSTI POS 500 DV_work_v2.xls.txt\t2
DB_Small_MASTER.txt\t2
Collaterals_AllPortfolios_Master.txt\t2
Assegnazioni_Master.txt\t2
ICCREA 3_Posizioni_Rilevani_Assolute_relative_31122024_work_v1.txt\t2
AuctionReport_Fortress_Master.txt\t2
Analisi Variazioni Gestito x Telasi_pad.txt\t2
20250519 Report Spese di Recupero_work.txt\t2
Insurance_Db_Fortress_2025_Master.txt\t2
Analisi Variazioni Gestito x Telasi_cred.txt\t2
01.Fatturone_MASTER_v3.txt\t2
FINO_Budget 2025 - Working - NEW.txt\t2
BCC_Npls_18-2_Loan by Loan_202506_work.txt\t2
Fonteno_AllegatoG_work.txt\t2
DB_Incassi_Delibere_sent.txt\t2
DB_Incassi_Delibere.txt\t2
TOULOSA_35246_SOF_DCR_311224_bd.txt\t1
SAL_Rendicontazioni_ITACA_V3_pivot.txt\t1
Reportistica_FINO_Procedure_not_CTU.txt\t1
PERIMETRO_FINPIEMONTE_ORIGINARIO_pivot.txt\t1
Master_Incassi_Luzzatti_21_new_v3.txt\t1
PERIMETRO_ITACA_APERTE_NUM_PRAT_CARICO L_pivot.txt\t1
Reportistica_FINO_Incassi_2021.txt\t1
PERIMETRO_OLYMPIA_APERTE_NUM_PRAT_CARICO L_pivot.txt\t1
S015_2025_Aste mai fissate_PX.txt\t1
Master_Sestante_Stresa_Work_2025.txt\t1
SAL_S017_2025_SURROGA IN PEI.txt\t1
PERIMETRO_ROMEO_MERCUZIO_PER GESTIONE_CARICO_L_pivot.txt\t1
Spese_FINO_Master.txt\t1
PERIMETRO_UCI_ARENA_APERTE_NUM_PRAT_CARICO L_pivot.txt\t1
POPs NPL 2021 - Updated Portfolio Base Case Scenario_20250414_sent.txt\t1
Reportistica_FINO_New_Attivazioni.txt\t1
Monitoraggi_Atti_MSA - report - MASTER.txt\t1
Reportistica_FINO_Property_DB.txt\t1
SAL_RENDICONTAZIONI CON ANOMALIE_FINO1_FINO2_pivot.txt\t1
SAL_Rendicontazioni_ROMEO_MERCUZIO_pivot.txt\t1
Luzzatti- Quarterly_DPO_draft.txt\t1
monitoraggio Arrears Sestante e Stresa_Master.txt\t1
PRATICAMag202506191030.txt\t1
Ortles - Semiannual_Aste_draft.txt\t1
WembleyLight_Master.txt\t1
Ortles - Semiannual_DPO_draft.txt\t1
Olympia - Quarterly_Aste_draft.txt\t1
Olympia - Quarterly_DPO_draft.txt\t1
Master_Dovams_Delibere Az Legale_2025.txt\t1
Luzzatti- Semiannual_DPO_draft.txt\t1
Reportistica_FINO_Lotti_Fino_Integrazion.txt\t1
Luzzatti- Semiannual_Aste_draft.txt\t1
Reportistica_FINO_Previsioni_Giudiz.txt\t1
Olympia - Semiannual_Aste_draft.txt\t1
Reportistica_FINO_ProcedureAperte.txt\t1
MASTER_PDR_NN Rispettati.txt\t1
Ortles - Quarterly_Aste_draft.txt\t1
Lotti_Fortress_integraz.txt\t1
S016_2025_Aste da rifissare_PX.txt\t1
report DETTAGLIO_UNICREDIT BANK AG_2021_pivot.txt\t1
SAL_Rendicontazioni_FINPIEMONTE_pivot.txt\t1
Ortles - Quarterly_DPO_draft.txt\t1
Olympia - Semiannual_DPO_draft.txt\t1
SAL_Rendicontazioni_UCI_ARENA_V3_pivot.txt\t1
Report Pratiche chiuse_lav.txt\t1
SAL_S018_2025_VERIFICA ANTIECONOMICITA' PEI.txt\t1
Report procedure  giudiziali NASSAU_20250625.txt\t1
Scheda mandato_MSA__master.txt\t1
Single name_2025.txt\t1
Westwood_33369_SOF_DCR_311224.txt\t1
Spese Semestre al 202503_work.txt\t1
MASTER_Cessioni_single name_v3.txt\t1
StandardPoors_Rating_ExIFAMS.txt\t1
Report stragiudiziali NASSAU_20250625.txt\t1
Monitoraggio Spese_ICCREA 3_work_v1_al 30042025.txt\t1
Monthly Report_BCC NPLs 2021_30062025.txt\t1
PCG_NoGACS_Lucrezia_Castiglione_Elab_202503_work.txt\t1
Olympia_Sample_2024.12_x_KAM.txt\t1
Luzzatti- Quarterly_Aste_draft.txt\t1
Report_Dettagli_Valutazioni_doRE-ARX_MASTER.txt\t1
WORK - DB per rapporti.txt\t1
Itaca - Quarterly_DPO_draft.txt\t1
Angera_spesedapagare_numprat.txt\t1
DB_Unico_2H_2024 xS&P.txt\t1
Fortress_Report_Sezione_Procedure_Master.txt\t1
Archiviati immobiliari_2023_2024.txt\t1
AVG_assegnazione_FY2024_v1.txt\t1
04.Notule_minore 200_da_Controllare_MASTER_v3.txt\t1
DB_Delibere.txt\t1
Dettaglio conferite_UCI_2025_Q1.txt\t1
LDTINTESTAgiu202507181030.txt\t1
02_Delibere_x_DoNext_master.txt\t1
BP2025_Presentazione Padovana.txt\t1
EPC_Recuperi_pddCessioni - New.txt\t1
ICCREA 5_Posizioni_Rilevani_Assolute_relative_31032025_work.txt\t1
Estrazione cespiti.txt\t1
INCASSI_MSA_2025.txt\t1
Collaterals_MPS_Master.txt\t1
IPOTECHE_20250531.txt\t1
Fineco PRATICHE DA RICHIAMARE DOVALUE 2024_work.txt\t1
LDTRAPPgiu202507181030.txt\t1
04.Notule_10k_da_Controllare_MASTER_v3.txt\t1
ITACA_report_incassi_20250531.txt\t1
Angera_IncassiSpese_DataCont.txt\t1
LDTPROCDELgiu202507181030.txt\t1
Fortress_Report_Angera_Depennate.txt\t1
BP2025_Presentazione Crediveneto.txt\t1
Fortress_Report_Angera_Incassi_E_Spese.txt\t1
By Ndg_Luzzatti_BP_2025.txt\t1
Fortress_Report_Auction.txt\t1
Crediveneto - Max data incasso.txt\t1
Fortress_Report_DPO_Angera_Detail_Master.txt\t1
Crediveneto_35247_SOF_DCR_311224.txt\t1
Fortress_Report_Garanti_Master.txt\t1
DB_Chiusure_MSA_Master.txt\t1
Fortress_Report_Incassi_Angera.txt\t1
Insurance_2025_DB_Unico_Output_Master.txt\t1
Fortress_Report_Incassi_Tribeca.txt\t1
Ipotecario_Non_Attivato_Fino.txt\t1
Fortress_Report_Ipotecario_Non_Attivato.txt\t1
Db_Delibere Tahiti.txt\t1
Fortress_Report_Previsioni_Giudiziali.txt\t1
DB_Gestione_Sestante e Stresa.txt\t1
Fortress_Report_Procedure_Aperte.txt\t1
Itaca - Quarterly_Aste_draft.txt\t1
Fortress_Report_Property_DB_Master.txt\t1
Castiglione_35247_SOF_DCR_311224.txt\t1
Fortress_Report_PropertyDB_Sibilla.txt\t1
LDTBENIgiu202507181030.txt\t1
Fortress_Report_Sezione_DPO_EPC_Master.txt\t1
LDTPERIZIEgiu202507181030.txt\t1
Fortress_Report_Sezione_Incassi_Master.txt\t1
Cessioni_2025_1H_20250626.txt\t1
Fortress_Report_Sezione_Pace_Master.txt\t1
DB Delibere Fino_20250617_da aggiornare db_invio.txt\t1
LDT_Master_Perim_ALL.txt\t28
LDT_Master_Perim.txt\t14
bp_db_macro.txt\t12
ICCREA 2_Monitoraggio NPV Profitability ratio_al 30062025_work_v1.txt\t10
ICCREA 3_Monitoraggio NPV Profitability ratio_al 30062025_work_v1.txt\t10
PM dashboard WIP_HoC_Primavera_master_v1.txt\t7
PM dashboard WIP_HoC_Palazzetti_master_v1.txt\t7
PM dashboard WIP_HoC_Del Manso_master_v1.txt\t7
PM dashboard WIP_AM_Automatico_v1.txt\t7
bp_db_macro_CRED.txt\t4
db_BCC2018-2.txt\t4
Astore_Tabelle@202505.txt\t3
KPI_Fino_Monitoring_BP2024@20250701_work.txt\t2
Report_Delibere_Pending_Palazzetti_Master_v7.txt\t2
SAL_BP_To_Do_2025_GACS_20250701.txt\t1
DB_ML_20250627.txt\t2
'''.splitlines()
user_dict = {line.split('\t')[0].strip(): int(line.split('\t')[1]) for line in user_counts if '\t' in line and line.split('\t')[1].strip().isdigit()}

# Ottieni info estratte
txt_only_info = bl_obj.get_txt_only_connection_info()


# Raggruppa per radice senza estensione

# Conta solo file unici per ogni prefisso nella tabella di riepilogo
prefix_found = {k.replace('.txt', ''): 0 for k in user_dict}
seen_per_prefix = {prefix: set() for prefix in prefix_found}
for fname, *_ in txt_only_info:
    for prefix in prefix_found:
        if fname.startswith(prefix) and fname not in seen_per_prefix[prefix]:
            prefix_found[prefix] += 1
            seen_per_prefix[prefix].add(fname)
            break


# Esporta la tabella di confronto in uno sheet Excel
summary_rows = []
for k in user_dict:
    prefix = k.replace('.txt', '')
    attese = user_dict[k]
    estratte = prefix_found.get(prefix, 0)
    status = 'OK' if estratte == attese else 'KO'
    summary_rows.append([prefix, attese, estratte, status])

columns_summary = ['Radice', 'Attese', 'Estratte', 'OK/KO']
stampa_report_connessioni.write_excel(columns_summary, summary_rows, sheet_name='File gestiti parzialmente')
