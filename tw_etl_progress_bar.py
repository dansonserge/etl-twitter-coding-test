import colorama

from numpy import number

def progress_bar(progress, total, color=colorama.Fore.YELLOW, description=""):
    percent = 100*(progress/float(total))
    bar = '▓' *  int(percent) + '-'* (100-int(percent))
    print(color + f"\r|{bar}| {percent:.2f}% {description}", end="\r",)
    if progress == total:
        print(colorama.Fore.GREEN + f"\r|{bar}|{percent:.2f}% {description} ")


