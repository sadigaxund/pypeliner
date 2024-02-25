import subprocess
import sys

processes = []
for service in ['orchestrator', 'extractor', 'reporter', 'apify']:
    command = [sys.executable, '-m', f'src.services.{service}']
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, stdin=subprocess.PIPE, shell=False)
    processes.append((service, process))
    
for process in processes:
    # Optionally, you can wait for the process to complete (block the current script until the subprocess finishes)
    process[1].wait()

    # Alternatively, you can use the following to check if the process is still running
    if process[1].poll() is None:
        print(f"Process [{process[0]}] is still running!")
        pass
    
    # Get the output and error streams if needed
    stdout, stderr = process[1].communicate()
    print("Output:", stdout.decode('utf-8'))
    print("Error:", stderr.decode('utf-8'))

# # Use subprocess.Popen to start the subprocess in the background


# Get the output and error streams if needed
# stdout, stderr = process.communicate()

# # Print the output and error (if any)
# print("Output:", stdout.decode('utf-8'))
# print("Error:", stderr.decode('utf-8'))


# subprocess.run([sys.executable, '-m', 'src.services.proxy'], stdout=subprocess.PIPE,
#                stderr=subprocess.PIPE, stdin=subprocess.PIPE, shell=False)
# subprocess.run([sys.executable, '-m', 'src.services.orchestrator'])
# subprocess.run([sys.executable, '-m', 'src.services.extractor'])
# subprocess.run([sys.executable, '-m', 'src.services.reporter'])
# subprocess.run([sys.executable, '-m', 'src.services.apify'])
