import logging
import logging.handlers

log = logging.getLogger("CRS")
log.setLevel(logging.DEBUG)
logh = logging.handlers.SysLogHandler(address="/dev/log")
log.addHandler(logh)

log.info("INFO")
log.debug("DEBUG")
