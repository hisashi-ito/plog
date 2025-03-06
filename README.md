# plog
Plog is a command-line utility that executes commands on multiple remote hosts concurrently using SSH. It reads a list of hosts and commands from specified files, runs the commands in parallel, and saves the output logs in a structured directory.

## Usage
```
python3 plog.py --host <host file> --cmds <cmds file> --nproc <number of processes> --output_dir <output_dir>
```

## Parameters
* host file:  
  A text file containing the list of hosts. Each line should include an optional label and the corresponding hostname or IP address, separated by a TAB. The program will SSH into each host listed in this file.

* cmds file:  
   A text file containing the commands to execute on the remote hosts. Each line should include an optional command label and the command itself, separated by a TAB.

* nproc:  
  The number of parallel processes to use during execution. This controls the level of concurrency using joblib.

* output_dir:  
  The directory path where the execution results will be stored. For each command, the output will be saved in the format:
  ```text
  <hostname>/<command label>_<execution time>.txt
  ```

## Additional Requirements
Make sure to install the required Python packages before running the script:
```
pip3 install joblib paramiko
```
