from migames.livequiz.functions import log_read

# List game codes to process
gc_list = []

for gc in gc_list:
    print(gc)
    logfile = "D:\\shizzz\\qa\\{0}.log".format(gc)
    jsonfile = "D:\\shizzz\\qa\\{0}.json".format(gc)
    log_read(logfile, jsonfile)
