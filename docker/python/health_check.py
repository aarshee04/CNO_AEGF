import psutil
import subprocess
import os
import configparser

scriptDir = os.path.dirname(os.path.abspath(__file__))

props = configparser.ConfigParser()
props.read(f"{scriptDir}/config.properties")
prgName = props.get("APP", "MSG_LIS_PRG")

psList = [p.cmdline()[1] for p in psutil.process_iter() if p.name().lower() in ["python3"]]

print(psList)

if not any(prgName.lower() in ps.lower() for ps in psList):
    print("Message listener is not running, starting the process")

    # Popen will run the long running process as child process without blocking the current program
    subprocess.Popen(["python3", f"{scriptDir}/message_listener.py"])
