import daemon
from QuoteInvoice import QuoteOrderInvoice

with daemon.DaemonContext():
    QuoteOrderInvoice()