from pathlib import Path

def makeDockerIgnore():
    #path to location of etransfer repo. Change if needed
    target_directory = Path("./eTransfer_Codebase")
    fileName = ".dockerignore"
    completePath = target_directory / fileName

    #list of all file paths which contain files not necessary for the container
    ignoredFiles = ['.vscode/\n', '.git/\n', '.env/\n', '*.tmp\n', '*.user\n', '*.log\n']

    output = open(completePath, "w")
    output.writelines(ignoredFiles)

makeDockerIgnore()