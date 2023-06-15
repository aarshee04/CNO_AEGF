import psutil
import subprocess
import os

scriptDir = os.path.dirname(os.path.abspath(__file__))
psList = [p.cmdline()[1] for p in psutil.process_iter() if p.name().lower() in ["python3"]]

print(psList)

if not any("schedulecronjobs.py" in ps.lower() for ps in psList):
    print("Scheduler is not running, starting the process")

    # Popen will run the long running process as child process without blocking the current program
    subprocess.Popen(["python3", f"{scriptDir}/scheduleCronjobs.py"])
