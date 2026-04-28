import sys
import subprocess

python = sys.executable
subprocess.run([python, 'push_to_blob.py'], check=True)
subprocess.run([python, 'push_to_database.py'], check=True)
