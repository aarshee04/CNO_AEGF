import signal
import time

run = True
sigList = [signal.SIGINT, signal.SIGHUP, signal.SIGKILL, signal.SIGQUIT, signal.SIGABRT, signal.SIGSTOP, signal.SIGTERM]

def signalHandler(sig, frame):
    global run
    run = False
    print(f"Caught signal {sig}")

for sig in sigList:
    signal.signal(signal.SIGINT, signalHandler)

while run:
    time.sleep(2)