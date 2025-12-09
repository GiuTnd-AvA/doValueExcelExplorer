class ExecPsCode():

    #costruttore che prende il path del file power shell
    def __init__(self, power_shell_file_path):
        self.power_shell_file_path = power_shell_file_path
    
    def run(self):
        import subprocess

        # Execute the PowerShell script
        process = subprocess.Popen(
            ["powershell", "-ExecutionPolicy", "Bypass", "-File", self.power_shell_file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Capture the output and errors
        output, error = process.communicate()

        if process.returncode != 0:
            raise Exception(f"PowerShell script failed with error: {error.strip()}")

        return process.returncode, output.strip(), error.strip()