
import zipfile
import xml.etree.ElementTree as ET

def estrai_m_da_xlsx(percorso_xlsx):
    with zipfile.ZipFile(percorso_xlsx, 'r') as z:
        for name in z.namelist():
            if name.startswith('xl/queries/') and name.endswith('.xml'):
                with z.open(name) as f:
                    root = ET.parse(f).getroot()
                    print(f"[{name}]")
                    # Alcuni file hanno nodi tipo <mc:AlternateContent> o <q:query> con child che includono M
                    # In assenza di uno schema fisso, stampiamo grezzo:
                    print(ET.tostring(root, encoding='unicode'))
                    print("-" * 60)
        if 'xl/customData/DataMashup' in z.namelist():
            print("Trovato xl/customData/DataMashup (binario proprietario).")
            print("Per decodificarlo, non c’è una libreria Python ufficiale; serve analisi custom o strumenti terzi.")

print(estrai_m_da_xlsx(r"C:\Users\giuseppe.tanda\Desktop\doValue\Report excel\Chargeability.xlsx"))