class ExecPsCode():
    # Costruttore che prende il path del file PowerShell e i parametri di percorso
    def __init__(self, power_shell_file_path, folder, export_mcode_path):
        # I parametri folder/export_mcode_path sono mantenuti per compatibilità,
        # ma lo script PS legge i valori da Config/config.ps1.
        self.power_shell_file_path = power_shell_file_path
        self.folder = folder
        self.export_mcode_path = export_mcode_path

    def run(self):
        import subprocess
        # Execute the PowerShell script con parametri
        process = subprocess.Popen(
            [
                "powershell", "-ExecutionPolicy", "Bypass", "-File", self.power_shell_file_path
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # Capture the output and errors
        output, error = process.communicate()
        if process.returncode != 0:
            raise Exception(f"PowerShell script failed with error: {error.strip()}")
        return process.returncode, output.strip(), error.strip()